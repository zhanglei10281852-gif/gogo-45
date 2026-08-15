package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"QueueForge/internal/model"
	"QueueForge/internal/store"
)

type FindingLevel string

const (
	LevelInfo    FindingLevel = "info"
	LevelWarning FindingLevel = "warning"
	LevelError   FindingLevel = "error"
)

type Finding struct {
	Level   FindingLevel `json:"level"`
	Code    string       `json:"code"`
	JobID   string       `json:"job_id,omitempty"`
	Message string       `json:"message"`
}

type Result struct {
	GeneratedAt time.Time          `json:"generated_at"`
	Journal     store.Verification `json:"journal"`
	Jobs        int                `json:"jobs"`
	Findings    []Finding          `json:"findings"`
	StateDigest string             `json:"state_digest"`
	Valid       bool               `json:"valid"`
}

func Inspect(jobs []*model.Job, journal store.Verification, now time.Time) Result {
	result := Result{GeneratedAt: now.UTC(), Journal: journal, Jobs: len(jobs), Valid: journal.Valid}
	byID := make(map[string]*model.Job, len(jobs))
	for _, job := range jobs {
		if job == nil {
			result.add(LevelError, "nil_job", "", "job collection contains nil entry")
			continue
		}
		if _, exists := byID[job.ID]; exists {
			result.add(LevelError, "duplicate_job", job.ID, "job id appears more than once")
		}
		byID[job.ID] = job
		if err := job.Validate(); err != nil {
			result.add(LevelError, "invalid_job", job.ID, err.Error())
		}
		inspectTiming(&result, job, now)
		inspectHistory(&result, job)
	}
	if err := model.ValidateDAG(byID); err != nil {
		result.add(LevelError, "invalid_dag", "", err.Error())
	}
	inspectIdempotency(&result, jobs)
	digest, err := StateDigest(jobs)
	if err != nil {
		result.add(LevelError, "digest_failed", "", err.Error())
	} else {
		result.StateDigest = digest
	}
	for _, finding := range result.Findings {
		if finding.Level == LevelError {
			result.Valid = false
		}
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		if result.Findings[i].Level != result.Findings[j].Level {
			return result.Findings[i].Level > result.Findings[j].Level
		}
		if result.Findings[i].JobID != result.Findings[j].JobID {
			return result.Findings[i].JobID < result.Findings[j].JobID
		}
		return result.Findings[i].Code < result.Findings[j].Code
	})
	return result
}

func (r *Result) add(level FindingLevel, code, jobID, message string) {
	r.Findings = append(r.Findings, Finding{Level: level, Code: code, JobID: jobID, Message: message})
}

func inspectTiming(result *Result, job *model.Job, now time.Time) {
	if job.CreatedAt.After(now.Add(time.Minute)) {
		result.add(LevelWarning, "future_creation", job.ID, "creation time is in the future")
	}
	if job.UpdatedAt.Before(job.CreatedAt) {
		result.add(LevelError, "update_before_creation", job.ID, "update time precedes creation")
	}
	if job.State == model.StateReady && job.AvailableAt.Before(now.Add(-24*time.Hour)) {
		result.add(LevelWarning, "stale_ready", job.ID, "job has been ready for more than 24 hours")
	}
	if job.State == model.StateRetryWait && !job.AvailableAt.After(now) {
		result.add(LevelWarning, "overdue_retry", job.ID, "retry delay elapsed but state was not refreshed")
	}
	if job.State == model.StateLeased && job.Lease != nil {
		if job.Lease.ExpiresAt.Before(now) {
			result.add(LevelWarning, "expired_lease", job.ID, "lease is expired and awaiting recovery")
		}
		if job.Lease.HeartbeatAt.Before(job.Lease.ClaimedAt) {
			result.add(LevelError, "heartbeat_before_claim", job.ID, "heartbeat precedes claim")
		}
		if job.Lease.ExpiresAt.Before(job.Lease.HeartbeatAt) {
			result.add(LevelError, "expiry_before_heartbeat", job.ID, "lease expiry precedes heartbeat")
		}
	}
	if job.Deadline != nil && job.State.Terminal() && job.UpdatedAt.After(*job.Deadline) {
		result.add(LevelInfo, "terminal_after_deadline", job.ID, "job reached terminal state after deadline")
	}
}

func inspectHistory(result *Result, job *model.Job) {
	if len(job.History) == 0 {
		result.add(LevelError, "empty_history", job.ID, "job has no transition history")
		return
	}
	previous := job.History[0]
	if previous.To == "" {
		result.add(LevelError, "invalid_initial_history", job.ID, "initial history has no state")
	}
	for index := 1; index < len(job.History); index++ {
		current := job.History[index]
		if current.At.Before(previous.At) {
			result.add(LevelError, "history_time_regression", job.ID, fmt.Sprintf("history entry %d precedes prior entry", index))
		}
		if current.From != previous.To {
			result.add(LevelError, "history_discontinuity", job.ID, fmt.Sprintf("history entry %d starts at %s, expected %s", index, current.From, previous.To))
		}
		if !model.CanTransition(current.From, current.To) {
			result.add(LevelError, "illegal_history_transition", job.ID, fmt.Sprintf("history entry %d has illegal transition", index))
		}
		previous = current
	}
	if previous.To != job.State {
		result.add(LevelError, "history_state_mismatch", job.ID, "final history state differs from job state")
	}
}
func inspectIdempotency(result *Result, jobs []*model.Job) {
	keys := make(map[string]string)
	for _, job := range jobs {
		if job == nil || job.IdempotencyKey == "" {
			continue
		}
		if prior, exists := keys[job.IdempotencyKey]; exists {
			result.add(LevelError, "duplicate_idempotency_key", job.ID, fmt.Sprintf("key is also used by %s", prior))
		}
		keys[job.IdempotencyKey] = job.ID
	}
}

func StateDigest(jobs []*model.Job) (string, error) {
	ordered := append([]*model.Job(nil), jobs...)
	for _, job := range ordered {
		if job == nil {
			return "", errors.New("cannot digest nil job")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	hash := sha256.New()
	for _, job := range ordered {
		data, err := json.Marshal(job)
		if err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(job.ID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func Text(result Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "audit valid: %t\n", result.Valid)
	fmt.Fprintf(&b, "jobs: %d\n", result.Jobs)
	fmt.Fprintf(&b, "journal events: %d\n", result.Journal.Events)
	fmt.Fprintf(&b, "journal head: %s\n", result.Journal.LastHash)
	fmt.Fprintf(&b, "state digest: %s\n", result.StateDigest)
	for _, finding := range result.Findings {
		if finding.JobID == "" {
			fmt.Fprintf(&b, "%s [%s] %s\n", finding.Level, finding.Code, finding.Message)
		} else {
			fmt.Fprintf(&b, "%s [%s] job=%s %s\n", finding.Level, finding.Code, finding.JobID, finding.Message)
		}
	}
	return b.String()
}
