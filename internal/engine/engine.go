package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/model"
	"QueueForge/internal/store"
)

type Clock interface{ Now() time.Time }
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

type Queue struct {
	cfg   config.Config
	store *store.Store
	clock Clock
}

func Open(cfg config.Config) (*Queue, error) {
	s, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}
	q := &Queue{cfg: cfg, store: s, clock: RealClock{}}
	if _, err := q.RecoverExpired(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return q, nil
}

func OpenWithClock(cfg config.Config, clock Clock) (*Queue, error) {
	s, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}
	return &Queue{cfg: cfg, store: s, clock: clock}, nil
}

func (q *Queue) Close() error { return q.store.Close() }

type EnqueueResult struct {
	Job       *model.Job `json:"job"`
	Duplicate bool       `json:"duplicate"`
}

func (q *Queue) Enqueue(request model.EnqueueRequest) (EnqueueResult, error) {
	now := q.clock.Now().UTC()
	if request.IdempotencyKey != "" {
		if record, ok := q.store.Idempotency(request.IdempotencyKey); ok {
			if now.Sub(record.CreatedAt) <= q.cfg.IdempotencyRetention() {
				job, found := q.store.Job(record.JobID)
				if !found {
					return EnqueueResult{}, fmt.Errorf("idempotency record refers to missing job %s", record.JobID)
				}
				return EnqueueResult{Job: job, Duplicate: true}, nil
			}
		}
	}
	job, err := q.prepareJob(request, now)
	if err != nil {
		return EnqueueResult{}, err
	}
	if err := q.store.Create(job); err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Job: model.CloneJob(job)}, nil
}

func (q *Queue) prepareJob(request model.EnqueueRequest, now time.Time) (*model.Job, error) {
	request.Queue = strings.TrimSpace(request.Queue)
	request.Type = strings.TrimSpace(request.Type)
	if request.Queue == "" {
		request.Queue = q.cfg.DefaultQueues[0]
	}
	if request.Type == "" {
		return nil, errors.New("type is required")
	}
	if len(request.Payload) == 0 {
		request.Payload = []byte("null")
	}
	if request.DelaySeconds < 0 {
		return nil, errors.New("delay_seconds must be nonnegative")
	}
	if request.AvailableAt != nil && request.DelaySeconds != 0 {
		return nil, errors.New("available_at and delay_seconds are mutually exclusive")
	}
	available := now
	if request.AvailableAt != nil {
		available = request.AvailableAt.UTC()
	}
	if request.DelaySeconds > 0 {
		available = now.Add(time.Duration(request.DelaySeconds) * time.Second)
	}
	if request.ID == "" {
		request.ID = generateID(now, request.Type, request.IdempotencyKey, len(q.store.Jobs()))
	}
	maxAttempts := request.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = q.cfg.DefaultMaxAttempts
	}
	backoff := model.BackoffPolicy{Kind: "exponential", BaseSeconds: q.cfg.DefaultBackoffSeconds, MaxSeconds: q.cfg.DefaultBackoffMaxSeconds, JitterPercent: 10}
	if request.Backoff != nil {
		backoff = *request.Backoff
	}
	resources := request.Resources
	if resources.Slots == 0 {
		resources.Slots = 1
	}
	state := model.StateReady
	reason := "enqueued and ready"
	if len(request.Dependencies) > 0 {
		state = model.StateBlocked
		reason = "waiting for dependencies"
	}
	job := &model.Job{
		ID: request.ID, Queue: request.Queue, Type: request.Type,
		Payload: append([]byte(nil), request.Payload...), Metadata: cloneStrings(request.Metadata),
		Priority: request.Priority, State: state, CreatedAt: now, UpdatedAt: now,
		AvailableAt: available, Deadline: cloneTime(request.Deadline),
		Dependencies: append([]string(nil), request.Dependencies...), MaxAttempts: maxAttempts,
		Backoff: backoff, RequiredLabels: cloneStrings(request.RequiredLabels),
		Resources: resources, IdempotencyKey: request.IdempotencyKey,
		History: []model.Transition{{To: state, At: now, Reason: reason, Actor: "enqueue"}},
	}
	if err := job.Validate(); err != nil {
		return nil, err
	}
	return job, nil
}

func generateID(now time.Time, kind, key string, count int) string {
	seed := fmt.Sprintf("%d\x00%s\x00%s\x00%d", now.UnixNano(), kind, key, count)
	digest := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("job-%d-%x", now.UnixMilli(), digest[:6])
}

func cloneStrings(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}
func (q *Queue) Claim(request model.ClaimRequest) ([]*model.Job, error) {
	if err := request.Worker.Validate(); err != nil {
		return nil, err
	}
	if request.Limit == 0 {
		request.Limit = request.Worker.Capacity.Slots
	}
	if request.Limit < 1 || request.Limit > 1000 {
		return nil, errors.New("limit must be between 1 and 1000")
	}
	leaseDuration := q.cfg.LeaseDuration()
	if request.LeaseSeconds != 0 {
		if request.LeaseSeconds < 1 || request.LeaseSeconds > 86400 {
			return nil, errors.New("lease_seconds must be between 1 and 86400")
		}
		leaseDuration = time.Duration(request.LeaseSeconds) * time.Second
	}
	now := q.clock.Now().UTC()
	if _, err := q.RecoverExpired(); err != nil {
		return nil, err
	}
	if _, err := q.Refresh(); err != nil {
		return nil, err
	}
	jobs := q.store.Jobs()
	candidates := make([]*model.Job, 0)
	for _, job := range jobs {
		if job.State != model.StateReady || job.AvailableAt.After(now) {
			continue
		}
		if job.Deadline != nil && !job.Deadline.After(now) {
			continue
		}
		candidates = append(candidates, job)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		if !candidates[i].AvailableAt.Equal(candidates[j].AvailableAt) {
			return candidates[i].AvailableAt.Before(candidates[j].AvailableAt)
		}
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].ID < candidates[j].ID
	})
	remaining := request.Worker.Capacity
	claimed := make([]*model.Job, 0, request.Limit)
	for _, job := range candidates {
		if len(claimed) >= request.Limit {
			break
		}
		if !request.Worker.Accepts(job, remaining) {
			continue
		}
		if err := job.Transition(model.StateLeased, now, "claimed", request.Worker.ID); err != nil {
			return nil, err
		}
		job.Attempts++
		job.Lease = &model.Lease{Token: leaseToken(job, request.Worker.ID, now), WorkerID: request.Worker.ID, ClaimedAt: now, HeartbeatAt: now, ExpiresAt: now.Add(leaseDuration)}
		remaining = remaining.Sub(job.Resources)
		claimed = append(claimed, job)
	}
	if err := q.store.UpdateMany(claimed, "jobs_claimed", now); err != nil {
		return nil, err
	}
	result := make([]*model.Job, len(claimed))
	for i, job := range claimed {
		result[i] = model.CloneJob(job)
	}
	return result, nil
}

func leaseToken(job *model.Job, worker string, now time.Time) string {
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, uint64(job.Attempts))
	digest := sha256.New()
	_, _ = digest.Write([]byte(job.ID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(worker))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(now.Format(time.RFC3339Nano)))
	_, _ = digest.Write(buffer)
	return fmt.Sprintf("lease-%x", digest.Sum(nil)[:16])
}

func (q *Queue) Heartbeat(request model.HeartbeatRequest) (*model.Job, error) {
	job, err := q.leasedJob(request.JobID, request.LeaseToken)
	if err != nil {
		return nil, err
	}
	now := q.clock.Now().UTC()
	if now.After(job.Lease.ExpiresAt.Add(q.cfg.HeartbeatGrace())) {
		return nil, errors.New("lease has expired")
	}
	extension := q.cfg.LeaseDuration()
	if request.ExtendSeconds != 0 {
		if request.ExtendSeconds < 1 || request.ExtendSeconds > 86400 {
			return nil, errors.New("extend_seconds must be between 1 and 86400")
		}
		extension = time.Duration(request.ExtendSeconds) * time.Second
	}
	job.Lease.HeartbeatAt = now
	job.Lease.ExpiresAt = now.Add(extension)
	job.UpdatedAt = now
	if err := q.store.Update(job, "job_heartbeat"); err != nil {
		return nil, err
	}
	return job, nil
}

func (q *Queue) leasedJob(id, token string) (*model.Job, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("job_id is required")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("lease_token is required")
	}
	job, ok := q.store.Job(id)
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	if job.State != model.StateLeased || job.Lease == nil {
		return nil, fmt.Errorf("job %q is not leased", id)
	}
	if job.Lease.Token != token {
		return nil, errors.New("lease token does not match")
	}
	return job, nil
}
func (q *Queue) Complete(request model.CompleteRequest) (*model.Job, error) {
	job, err := q.leasedJob(request.JobID, request.LeaseToken)
	if err != nil {
		return nil, err
	}
	now := q.clock.Now().UTC()
	if now.After(job.Lease.ExpiresAt.Add(q.cfg.HeartbeatGrace())) {
		return nil, errors.New("lease has expired")
	}
	actor := job.Lease.WorkerID
	job.Lease = nil
	job.Result = append([]byte(nil), request.Result...)
	if len(job.Result) == 0 {
		job.Result = []byte("null")
	}
	if err := job.Transition(model.StateSucceeded, now, "completed", actor); err != nil {
		return nil, err
	}
	if err := q.store.Update(job, "job_completed"); err != nil {
		return nil, err
	}
	if _, err := q.Refresh(); err != nil {
		return nil, err
	}
	return job, nil
}

func (q *Queue) Fail(request model.FailRequest) (*model.Job, error) {
	job, err := q.leasedJob(request.JobID, request.LeaseToken)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.Code) == "" {
		return nil, errors.New("code is required")
	}
	if strings.TrimSpace(request.Message) == "" {
		return nil, errors.New("message is required")
	}
	now := q.clock.Now().UTC()
	actor := job.Lease.WorkerID
	job.Lease = nil
	job.LastError = &model.Failure{Code: request.Code, Message: request.Message, Retryable: request.Retryable, At: now}
	if request.Retryable && job.Attempts < job.MaxAttempts {
		delay := backoffDelay(job.Backoff, job.Attempts, job.ID)
		job.AvailableAt = now.Add(delay)
		if err := job.Transition(model.StateRetryWait, now, fmt.Sprintf("retry scheduled in %s", delay), actor); err != nil {
			return nil, err
		}
	} else {
		reason := "non-retryable failure"
		if request.Retryable {
			reason = "attempt limit reached"
		}
		if err := job.Transition(model.StateDead, now, reason, actor); err != nil {
			return nil, err
		}
	}
	if err := q.store.Update(job, "job_failed"); err != nil {
		return nil, err
	}
	if job.State == model.StateDead {
		_, err = q.Refresh()
	}
	return job, err
}

func backoffDelay(policy model.BackoffPolicy, attempt int, id string) time.Duration {
	base := policy.BaseSeconds
	var seconds int64
	switch policy.Kind {
	case "fixed":
		seconds = base
	case "linear":
		seconds = base * int64(attempt)
	default:
		seconds = base
		for i := 1; i < attempt && seconds < policy.MaxSeconds; i++ {
			if seconds > policy.MaxSeconds/2 {
				seconds = policy.MaxSeconds
				break
			}
			seconds *= 2
		}
	}
	if seconds > policy.MaxSeconds {
		seconds = policy.MaxSeconds
	}
	if policy.JitterPercent > 0 && seconds > 0 {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", id, attempt)))
		span := seconds * int64(policy.JitterPercent) / 100
		if span > 0 {
			offset := int64(binary.BigEndian.Uint64(digest[:8])%uint64(span*2+1)) - span
			seconds += offset
		}
	}
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(seconds) * time.Second
}

func (q *Queue) RecoverExpired() ([]*model.Job, error) {
	now := q.clock.Now().UTC()
	jobs := q.store.Jobs()
	changed := make([]*model.Job, 0)
	for _, job := range jobs {
		if job.State != model.StateLeased || job.Lease == nil {
			continue
		}
		if now.Before(job.Lease.ExpiresAt.Add(q.cfg.HeartbeatGrace())) {
			continue
		}
		worker := job.Lease.WorkerID
		job.Lease = nil
		job.LastError = &model.Failure{Code: "lease_expired", Message: "worker lease expired", Retryable: true, At: now}
		if job.Attempts < job.MaxAttempts {
			job.AvailableAt = now.Add(backoffDelay(job.Backoff, job.Attempts, job.ID))
			if err := job.Transition(model.StateRetryWait, now, "expired lease scheduled for retry", worker); err != nil {
				return nil, err
			}
		} else {
			if err := job.Transition(model.StateDead, now, "expired lease exhausted attempts", worker); err != nil {
				return nil, err
			}
		}
		changed = append(changed, job)
	}
	if err := q.store.UpdateMany(changed, "jobs_recovered", now); err != nil {
		return nil, err
	}
	return changed, nil
}
func (q *Queue) Refresh() ([]*model.Job, error) {
	now := q.clock.Now().UTC()
	jobs := q.store.Jobs()
	changed := make([]*model.Job, 0)
	ids := make([]string, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		job := jobs[id]
		switch job.State {
		case model.StateRetryWait:
			if !job.AvailableAt.After(now) {
				if err := job.Transition(model.StateReady, now, "retry delay elapsed", "scheduler"); err != nil {
					return nil, err
				}
				changed = append(changed, job)
			}
		case model.StateBlocked:
			satisfied, reason := model.DependenciesSatisfied(job, jobs)
			if satisfied {
				if err := job.Transition(model.StateReady, now, reason, "scheduler"); err != nil {
					return nil, err
				}
				changed = append(changed, job)
			} else if strings.HasPrefix(reason, "failed dependency") {
				job.LastError = &model.Failure{Code: "dependency_failed", Message: reason, Retryable: false, At: now}
				if err := job.Transition(model.StateDead, now, reason, "scheduler"); err != nil {
					return nil, err
				}
				changed = append(changed, job)
			}
		case model.StateReady:
			if job.Deadline != nil && !job.Deadline.After(now) {
				job.LastError = &model.Failure{Code: "deadline_exceeded", Message: "job deadline elapsed before claim", Retryable: false, At: now}
				if err := job.Transition(model.StateDead, now, "deadline exceeded", "scheduler"); err != nil {
					return nil, err
				}
				changed = append(changed, job)
			}
		}
	}
	if err := q.store.UpdateMany(changed, "jobs_refreshed", now); err != nil {
		return nil, err
	}
	return changed, nil
}

func (q *Queue) Jobs() []*model.Job {
	jobs := q.store.Jobs()
	result := make([]*model.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func (q *Queue) Job(id string) (*model.Job, bool) { return q.store.Job(id) }
func (q *Queue) Snapshot() error                  { return q.store.Snapshot() }

func (q *Queue) Validate() error {
	jobs := q.store.Jobs()
	for id, job := range jobs {
		if err := job.Validate(); err != nil {
			return fmt.Errorf("job %s: %w", id, err)
		}
	}
	return model.ValidateDAG(jobs)
}

func (q *Queue) CapacityUsage(worker model.Worker) (model.Resources, error) {
	if err := worker.Validate(); err != nil {
		return model.Resources{}, err
	}
	usage := model.Resources{}
	for _, job := range q.Jobs() {
		if job.State == model.StateLeased && job.Lease != nil && job.Lease.WorkerID == worker.ID {
			usage = usage.Add(job.Resources)
		}
	}
	return usage, nil
}

func (q *Queue) AvailableCapacity(worker model.Worker) (model.Resources, error) {
	usage, err := q.CapacityUsage(worker)
	if err != nil {
		return model.Resources{}, err
	}
	remaining := worker.Capacity.Sub(usage)
	if remaining.CPU < 0 || remaining.MemoryMB < 0 || remaining.Slots < 0 {
		return model.Resources{}, errors.New("worker is over capacity")
	}
	return remaining, nil
}

func (q *Queue) Audit() (store.Verification, error) { return store.Verify(q.cfg.JournalPath()) }
