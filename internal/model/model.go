package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type State string

const (
	StateBlocked   State = "blocked"
	StateReady     State = "ready"
	StateLeased    State = "leased"
	StateRetryWait State = "retry_wait"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

var allStates = map[State]bool{
	StateBlocked: true, StateReady: true, StateLeased: true,
	StateRetryWait: true, StateSucceeded: true, StateDead: true,
	StateCancelled: true,
}

type BackoffPolicy struct {
	Kind          string `json:"kind"`
	BaseSeconds   int64  `json:"base_seconds"`
	MaxSeconds    int64  `json:"max_seconds"`
	JitterPercent int    `json:"jitter_percent"`
}

type Resources struct {
	CPU      int `json:"cpu"`
	MemoryMB int `json:"memory_mb"`
	Slots    int `json:"slots"`
}
type Job struct {
	ID             string            `json:"id"`
	Queue          string            `json:"queue"`
	Type           string            `json:"type"`
	Payload        json.RawMessage   `json:"payload"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Priority       int               `json:"priority"`
	State          State             `json:"state"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	AvailableAt    time.Time         `json:"available_at"`
	Deadline       *time.Time        `json:"deadline,omitempty"`
	Dependencies   []string          `json:"dependencies,omitempty"`
	MaxAttempts    int               `json:"max_attempts"`
	Attempts       int               `json:"attempts"`
	Backoff        BackoffPolicy     `json:"backoff"`
	RequiredLabels map[string]string `json:"required_labels,omitempty"`
	Resources      Resources         `json:"resources"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Lease          *Lease            `json:"lease,omitempty"`
	Result         json.RawMessage   `json:"result,omitempty"`
	LastError      *Failure          `json:"last_error,omitempty"`
	History        []Transition      `json:"history"`
}

type Lease struct {
	Token       string    `json:"token"`
	WorkerID    string    `json:"worker_id"`
	ClaimedAt   time.Time `json:"claimed_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type Failure struct {
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
	At        time.Time `json:"at"`
}

type Transition struct {
	From   State     `json:"from,omitempty"`
	To     State     `json:"to"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
	Actor  string    `json:"actor,omitempty"`
}

type EnqueueRequest struct {
	ID             string            `json:"id,omitempty"`
	Queue          string            `json:"queue"`
	Type           string            `json:"type"`
	Payload        json.RawMessage   `json:"payload"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	AvailableAt    *time.Time        `json:"available_at,omitempty"`
	DelaySeconds   int64             `json:"delay_seconds,omitempty"`
	Deadline       *time.Time        `json:"deadline,omitempty"`
	Dependencies   []string          `json:"dependencies,omitempty"`
	MaxAttempts    int               `json:"max_attempts,omitempty"`
	Backoff        *BackoffPolicy    `json:"backoff,omitempty"`
	RequiredLabels map[string]string `json:"required_labels,omitempty"`
	Resources      Resources         `json:"resources,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

type Worker struct {
	ID       string            `json:"id"`
	Queues   []string          `json:"queues"`
	Labels   map[string]string `json:"labels,omitempty"`
	Capacity Resources         `json:"capacity"`
}

type ClaimRequest struct {
	Worker       Worker `json:"worker"`
	Limit        int    `json:"limit,omitempty"`
	LeaseSeconds int64  `json:"lease_seconds,omitempty"`
}

type HeartbeatRequest struct {
	JobID         string `json:"job_id"`
	LeaseToken    string `json:"lease_token"`
	ExtendSeconds int64  `json:"extend_seconds,omitempty"`
}

type CompleteRequest struct {
	JobID      string          `json:"job_id"`
	LeaseToken string          `json:"lease_token"`
	Result     json.RawMessage `json:"result,omitempty"`
}

type FailRequest struct {
	JobID      string `json:"job_id"`
	LeaseToken string `json:"lease_token"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable"`
}

func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateDead || s == StateCancelled
}

func (s State) Valid() bool { return allStates[s] }

func CanTransition(from, to State) bool {
	allowed := map[State]map[State]bool{
		StateBlocked:   {StateReady: true, StateCancelled: true, StateDead: true},
		StateReady:     {StateLeased: true, StateCancelled: true, StateDead: true},
		StateLeased:    {StateSucceeded: true, StateRetryWait: true, StateDead: true, StateReady: true},
		StateRetryWait: {StateReady: true, StateCancelled: true, StateDead: true},
		StateSucceeded: {}, StateDead: {}, StateCancelled: {},
	}
	return allowed[from][to]
}

func (j *Job) Transition(to State, at time.Time, reason, actor string) error {
	if j == nil {
		return errors.New("nil job")
	}
	if !to.Valid() {
		return fmt.Errorf("invalid destination state %q", to)
	}
	if !CanTransition(j.State, to) {
		return fmt.Errorf("transition %s -> %s is not allowed", j.State, to)
	}
	from := j.State
	j.State = to
	j.UpdatedAt = at.UTC()
	j.History = append(j.History, Transition{From: from, To: to, At: at.UTC(), Reason: reason, Actor: actor})
	return nil
}

func (j *Job) Validate() error {
	if j == nil {
		return errors.New("job is nil")
	}
	var problems []string
	if strings.TrimSpace(j.ID) == "" {
		problems = append(problems, "id is required")
	}
	if strings.TrimSpace(j.Queue) == "" {
		problems = append(problems, "queue is required")
	}
	if strings.TrimSpace(j.Type) == "" {
		problems = append(problems, "type is required")
	}
	if !j.State.Valid() {
		problems = append(problems, "state is invalid")
	}
	if j.Priority < -1000 || j.Priority > 1000 {
		problems = append(problems, "priority must be between -1000 and 1000")
	}
	if j.MaxAttempts < 1 || j.MaxAttempts > 1000 {
		problems = append(problems, "max_attempts must be between 1 and 1000")
	}
	if j.Attempts < 0 || j.Attempts > j.MaxAttempts {
		problems = append(problems, "attempts is outside valid range")
	}
	if err := j.Backoff.Validate(); err != nil {
		problems = append(problems, err.Error())
	}
	if err := j.Resources.Validate(); err != nil {
		problems = append(problems, err.Error())
	}
	if len(j.Payload) == 0 || !json.Valid(j.Payload) {
		problems = append(problems, "payload must be valid JSON")
	}
	if j.Deadline != nil && j.Deadline.Before(j.CreatedAt) {
		problems = append(problems, "deadline precedes creation")
	}
	if j.Lease != nil && j.State != StateLeased {
		problems = append(problems, "only leased jobs may have a lease")
	}
	if j.State == StateLeased && j.Lease == nil {
		problems = append(problems, "leased job has no lease")
	}
	seen := make(map[string]bool)
	for _, dep := range j.Dependencies {
		if dep == "" {
			problems = append(problems, "dependency id is empty")
		}
		if dep == j.ID {
			problems = append(problems, "job cannot depend on itself")
		}
		if seen[dep] {
			problems = append(problems, "duplicate dependency "+dep)
		}
		seen[dep] = true
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (p BackoffPolicy) Validate() error {
	if p.Kind != "fixed" && p.Kind != "linear" && p.Kind != "exponential" {
		return fmt.Errorf("backoff kind must be fixed, linear, or exponential")
	}
	if p.BaseSeconds < 0 {
		return errors.New("backoff base_seconds must be nonnegative")
	}
	if p.MaxSeconds < p.BaseSeconds {
		return errors.New("backoff max_seconds must be at least base_seconds")
	}
	if p.JitterPercent < 0 || p.JitterPercent > 100 {
		return errors.New("backoff jitter_percent must be between 0 and 100")
	}
	return nil
}

func (r Resources) Validate() error {
	if r.CPU < 0 || r.MemoryMB < 0 || r.Slots < 0 {
		return errors.New("resource values must be nonnegative")
	}
	if r.CPU > 1_000_000 || r.MemoryMB > 1_000_000_000 || r.Slots > 1_000_000 {
		return errors.New("resource value exceeds safety limit")
	}
	return nil
}
func (r Resources) Fits(capacity Resources) bool {
	return r.CPU <= capacity.CPU && r.MemoryMB <= capacity.MemoryMB && r.Slots <= capacity.Slots
}

func (r Resources) Add(other Resources) Resources {
	return Resources{CPU: r.CPU + other.CPU, MemoryMB: r.MemoryMB + other.MemoryMB, Slots: r.Slots + other.Slots}
}

func (r Resources) Sub(other Resources) Resources {
	return Resources{CPU: r.CPU - other.CPU, MemoryMB: r.MemoryMB - other.MemoryMB, Slots: r.Slots - other.Slots}
}

func (w Worker) Validate() error {
	var problems []string
	if strings.TrimSpace(w.ID) == "" {
		problems = append(problems, "worker id is required")
	}
	if len(w.Queues) == 0 {
		problems = append(problems, "worker must subscribe to a queue")
	}
	if err := w.Capacity.Validate(); err != nil {
		problems = append(problems, err.Error())
	}
	if w.Capacity.Slots < 1 {
		problems = append(problems, "worker slots must be positive")
	}
	seen := make(map[string]bool)
	for _, q := range w.Queues {
		if strings.TrimSpace(q) == "" {
			problems = append(problems, "worker queue is empty")
		}
		if seen[q] {
			problems = append(problems, "duplicate worker queue "+q)
		}
		seen[q] = true
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (w Worker) Accepts(j *Job, remaining Resources) bool {
	if j == nil || !j.Resources.Fits(remaining) {
		return false
	}
	queueOK := false
	for _, q := range w.Queues {
		if q == j.Queue || q == "*" {
			queueOK = true
			break
		}
	}
	if !queueOK {
		return false
	}
	for key, value := range j.RequiredLabels {
		if w.Labels[key] != value {
			return false
		}
	}
	return true
}

func ValidateDAG(jobs map[string]*Job) error {
	colors := make(map[string]uint8, len(jobs))
	stack := make([]string, 0)
	var visit func(string) error
	visit = func(id string) error {
		switch colors[id] {
		case 1:
			start := 0
			for i, item := range stack {
				if item == id {
					start = i
					break
				}
			}
			cycle := append(append([]string{}, stack[start:]...), id)
			return fmt.Errorf("dependency cycle: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}
		job, ok := jobs[id]
		if !ok {
			return fmt.Errorf("job %q does not exist", id)
		}
		colors[id] = 1
		stack = append(stack, id)
		for _, dep := range job.Dependencies {
			if _, exists := jobs[dep]; !exists {
				return fmt.Errorf("job %q depends on missing job %q", id, dep)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		colors[id] = 2
		return nil
	}
	ids := make([]string, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func DependenciesSatisfied(j *Job, jobs map[string]*Job) (bool, string) {
	for _, id := range j.Dependencies {
		dep, ok := jobs[id]
		if !ok {
			return false, "missing dependency " + id
		}
		if dep.State == StateDead || dep.State == StateCancelled {
			return false, "failed dependency " + id
		}
		if dep.State != StateSucceeded {
			return false, "waiting for dependency " + id
		}
	}
	return true, "dependencies satisfied"
}

func CloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	data, _ := json.Marshal(job)
	var clone Job
	_ = json.Unmarshal(data, &clone)
	return &clone
}

func CloneJobs(jobs map[string]*Job) map[string]*Job {
	clones := make(map[string]*Job, len(jobs))
	for id, job := range jobs {
		clones[id] = CloneJob(job)
	}
	return clones
}
