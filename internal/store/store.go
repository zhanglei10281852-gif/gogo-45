package store

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/model"
)

const journalVersion = 1

type Event struct {
	Version      int             `json:"version"`
	Sequence     uint64          `json:"sequence"`
	Time         time.Time       `json:"time"`
	Type         string          `json:"type"`
	JobID        string          `json:"job_id,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	PreviousHash string          `json:"previous_hash"`
	Hash         string          `json:"hash"`
}

type Snapshot struct {
	Version     int                          `json:"version"`
	Sequence    uint64                       `json:"sequence"`
	LastHash    string                       `json:"last_hash"`
	CreatedAt   time.Time                    `json:"created_at"`
	Jobs        map[string]*model.Job        `json:"jobs"`
	Idempotency map[string]IdempotencyRecord `json:"idempotency"`
}

type IdempotencyRecord struct {
	JobID     string    `json:"job_id"`
	CreatedAt time.Time `json:"created_at"`
}
type Store struct {
	mu                  sync.RWMutex
	cfg                 config.Config
	journal             *os.File
	lockPath            string
	jobs                map[string]*model.Job
	idempotency         map[string]IdempotencyRecord
	sequence            uint64
	lastHash            string
	eventsSinceSnapshot int
	closed              bool
}

type Verification struct {
	Events           int    `json:"events"`
	FirstSequence    uint64 `json:"first_sequence"`
	LastSequence     uint64 `json:"last_sequence"`
	LastHash         string `json:"last_hash"`
	SnapshotSequence uint64 `json:"snapshot_sequence"`
	Valid            bool   `json:"valid"`
}

type Recovery struct {
	EventsReplayed int          `json:"events_replayed"`
	JobsRecovered  int          `json:"jobs_recovered"`
	SnapshotUsed   bool         `json:"snapshot_used"`
	Verification   Verification `json:"verification"`
}

func Open(cfg config.Config) (*Store, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	lockPath := cfg.LockPath()
	lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("queue is locked by another process; remove %s only if no process is active", lockPath)
		}
		return nil, fmt.Errorf("acquire lock: %w", err)
	}
	_, _ = fmt.Fprintf(lock, "%d\n", os.Getpid())
	_ = lock.Close()
	s := &Store{cfg: cfg, lockPath: lockPath, jobs: make(map[string]*model.Job), idempotency: make(map[string]IdempotencyRecord)}
	if _, err := s.recoverLocked(); err != nil {
		_ = os.Remove(lockPath)
		return nil, err
	}
	journal, err := os.OpenFile(cfg.JournalPath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		_ = os.Remove(lockPath)
		return nil, fmt.Errorf("open journal: %w", err)
	}
	s.journal = journal
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if s.journal != nil {
		if err := s.journal.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := s.journal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := os.Remove(s.lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *Store) Jobs() map[string]*model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.CloneJobs(s.jobs)
}

func (s *Store) Job(id string) (*model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return model.CloneJob(job), ok
}

func (s *Store) Idempotency(key string) (IdempotencyRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.idempotency[key]
	return record, ok
}
func (s *Store) Create(job *model.Job) error {
	if job == nil {
		return errors.New("cannot create nil job")
	}
	if err := job.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return fmt.Errorf("job %q already exists", job.ID)
	}
	candidate := model.CloneJobs(s.jobs)
	candidate[job.ID] = model.CloneJob(job)
	if err := model.ValidateDAG(candidate); err != nil {
		return err
	}
	if job.IdempotencyKey != "" {
		if record, exists := s.idempotency[job.IdempotencyKey]; exists {
			return fmt.Errorf("idempotency key already belongs to job %s", record.JobID)
		}
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := s.appendLocked("job_created", job.ID, data, job.UpdatedAt); err != nil {
		return err
	}
	s.jobs[job.ID] = model.CloneJob(job)
	if job.IdempotencyKey != "" {
		s.idempotency[job.IdempotencyKey] = IdempotencyRecord{JobID: job.ID, CreatedAt: job.CreatedAt}
	}
	return s.maybeSnapshotLocked()
}

func (s *Store) Update(job *model.Job, eventType string) error {
	if job == nil {
		return errors.New("cannot update nil job")
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(eventType) == "" {
		return errors.New("event type is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.jobs[job.ID]
	if !exists {
		return fmt.Errorf("job %q not found", job.ID)
	}
	if old.IdempotencyKey != job.IdempotencyKey {
		return errors.New("idempotency key cannot change")
	}
	candidate := model.CloneJobs(s.jobs)
	candidate[job.ID] = model.CloneJob(job)
	if err := model.ValidateDAG(candidate); err != nil {
		return err
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := s.appendLocked(eventType, job.ID, data, job.UpdatedAt); err != nil {
		return err
	}
	s.jobs[job.ID] = model.CloneJob(job)
	return s.maybeSnapshotLocked()
}

func (s *Store) UpdateMany(jobs []*model.Job, eventType string, at time.Time) error {
	if len(jobs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := model.CloneJobs(s.jobs)
	for _, job := range jobs {
		if job == nil {
			return errors.New("cannot update nil job")
		}
		if err := job.Validate(); err != nil {
			return fmt.Errorf("job %s: %w", job.ID, err)
		}
		old, exists := s.jobs[job.ID]
		if !exists {
			return fmt.Errorf("job %q not found", job.ID)
		}
		if old.IdempotencyKey != job.IdempotencyKey {
			return errors.New("idempotency key cannot change")
		}
		candidate[job.ID] = model.CloneJob(job)
	}
	if err := model.ValidateDAG(candidate); err != nil {
		return err
	}
	data, err := json.Marshal(jobs)
	if err != nil {
		return err
	}
	if err := s.appendLocked(eventType, "", data, at); err != nil {
		return err
	}
	for _, job := range jobs {
		s.jobs[job.ID] = model.CloneJob(job)
	}
	return s.maybeSnapshotLocked()
}

func (s *Store) appendLocked(eventType, jobID string, data []byte, at time.Time) error {
	if s.closed {
		return errors.New("store is closed")
	}
	if s.journal == nil {
		return errors.New("journal is not open")
	}
	info, err := s.journal.Stat()
	if err != nil {
		return err
	}
	if info.Size()+int64(len(data))+1024 > s.cfg.MaxJournalBytes {
		return fmt.Errorf("journal size limit of %d bytes reached; create snapshot and archive", s.cfg.MaxJournalBytes)
	}
	e := Event{Version: journalVersion, Sequence: s.sequence + 1, Time: at.UTC(), Type: eventType, JobID: jobID, Data: append(json.RawMessage(nil), data...), PreviousHash: s.lastHash}
	e.Hash, err = eventHash(e)
	if err != nil {
		return err
	}
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := s.journal.Write(line); err != nil {
		return fmt.Errorf("append journal: %w", err)
	}
	if err := s.journal.Sync(); err != nil {
		return fmt.Errorf("sync journal: %w", err)
	}
	s.sequence = e.Sequence
	s.lastHash = e.Hash
	s.eventsSinceSnapshot++
	return nil
}
func eventHash(e Event) (string, error) {
	unsigned := struct {
		Version      int             `json:"version"`
		Sequence     uint64          `json:"sequence"`
		Time         time.Time       `json:"time"`
		Type         string          `json:"type"`
		JobID        string          `json:"job_id,omitempty"`
		Data         json.RawMessage `json:"data,omitempty"`
		PreviousHash string          `json:"previous_hash"`
	}{e.Version, e.Sequence, e.Time, e.Type, e.JobID, e.Data, e.PreviousHash}
	canonical, err := json.Marshal(unsigned)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func (s *Store) maybeSnapshotLocked() error {
	if s.eventsSinceSnapshot < s.cfg.SnapshotEvery {
		return nil
	}
	return s.snapshotLocked()
}

func (s *Store) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Store) snapshotLocked() error {
	snapshot := Snapshot{Version: 1, Sequence: s.sequence, LastHash: s.lastHash, CreatedAt: time.Now().UTC(), Jobs: model.CloneJobs(s.jobs), Idempotency: cloneIdempotency(s.idempotency)}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary := s.cfg.SnapshotPath() + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := replaceFile(temporary, s.cfg.SnapshotPath()); err != nil {
		return fmt.Errorf("install snapshot: %w", err)
	}
	ok = true
	s.eventsSinceSnapshot = 0
	return nil
}

func replaceFile(source, destination string) error {
	if err := os.Rename(source, destination); err == nil {
		return nil
	}
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(source, destination)
}

func cloneIdempotency(source map[string]IdempotencyRecord) map[string]IdempotencyRecord {
	result := make(map[string]IdempotencyRecord, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (s *Store) recoverLocked() (Recovery, error) {
	recovery := Recovery{}
	if err := s.loadSnapshotLocked(&recovery); err != nil {
		return recovery, err
	}
	verification, events, err := readAndVerifyJournal(s.cfg.JournalPath())
	if err != nil {
		return recovery, err
	}
	recovery.Verification = verification
	for _, event := range events {
		if event.Sequence <= s.sequence {
			continue
		}
		if event.PreviousHash != s.lastHash {
			return recovery, fmt.Errorf("journal event %d does not continue snapshot hash", event.Sequence)
		}
		if err := s.applyEventLocked(event); err != nil {
			return recovery, fmt.Errorf("replay event %d: %w", event.Sequence, err)
		}
		s.sequence = event.Sequence
		s.lastHash = event.Hash
		recovery.EventsReplayed++
	}
	if err := model.ValidateDAG(s.jobs); err != nil {
		return recovery, fmt.Errorf("recovered dependency graph: %w", err)
	}
	recovery.JobsRecovered = len(s.jobs)
	return recovery, nil
}
func (s *Store) loadSnapshotLocked(recovery *Recovery) error {
	data, err := os.ReadFile(s.cfg.SnapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var snapshot Snapshot
	if err := dec.Decode(&snapshot); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if snapshot.Version != 1 {
		return fmt.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	if snapshot.Jobs == nil {
		snapshot.Jobs = make(map[string]*model.Job)
	}
	if snapshot.Idempotency == nil {
		snapshot.Idempotency = make(map[string]IdempotencyRecord)
	}
	for id, job := range snapshot.Jobs {
		if id != job.ID {
			return fmt.Errorf("snapshot key %q differs from job id %q", id, job.ID)
		}
		if err := job.Validate(); err != nil {
			return fmt.Errorf("snapshot job %s: %w", id, err)
		}
	}
	if err := model.ValidateDAG(snapshot.Jobs); err != nil {
		return err
	}
	s.jobs = model.CloneJobs(snapshot.Jobs)
	s.idempotency = cloneIdempotency(snapshot.Idempotency)
	s.sequence = snapshot.Sequence
	s.lastHash = snapshot.LastHash
	recovery.SnapshotUsed = true
	recovery.Verification.SnapshotSequence = snapshot.Sequence
	return nil
}

func readAndVerifyJournal(path string) (Verification, []Event, error) {
	verification := Verification{Valid: true}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return verification, nil, nil
	}
	if err != nil {
		return verification, nil, fmt.Errorf("open journal: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var events []Event
	var previous string
	var sequence uint64
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			return verification, nil, fmt.Errorf("journal line %d is blank", lineNumber)
		}
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		var event Event
		if err := dec.Decode(&event); err != nil {
			return verification, nil, fmt.Errorf("journal line %d: %w", lineNumber, err)
		}
		if event.Version != journalVersion {
			return verification, nil, fmt.Errorf("journal line %d has unsupported version %d", lineNumber, event.Version)
		}
		if event.Sequence != sequence+1 {
			return verification, nil, fmt.Errorf("journal line %d sequence %d follows %d", lineNumber, event.Sequence, sequence)
		}
		if event.PreviousHash != previous {
			return verification, nil, fmt.Errorf("journal line %d previous hash mismatch", lineNumber)
		}
		expected, err := eventHash(event)
		if err != nil {
			return verification, nil, err
		}
		if !strings.EqualFold(expected, event.Hash) {
			return verification, nil, fmt.Errorf("journal line %d hash mismatch", lineNumber)
		}
		if verification.Events == 0 {
			verification.FirstSequence = event.Sequence
		}
		verification.Events++
		verification.LastSequence = event.Sequence
		verification.LastHash = event.Hash
		previous = event.Hash
		sequence = event.Sequence
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return verification, nil, fmt.Errorf("scan journal: %w", err)
	}
	return verification, events, nil
}

func Verify(path string) (Verification, error) {
	verification, _, err := readAndVerifyJournal(path)
	return verification, err
}
func (s *Store) applyEventLocked(event Event) error {
	switch event.Type {
	case "job_created", "job_claimed", "job_heartbeat", "job_completed", "job_failed", "job_released", "job_unblocked", "job_dependency_failed", "job_delay_elapsed":
		var job model.Job
		if err := json.Unmarshal(event.Data, &job); err != nil {
			return err
		}
		if event.JobID != "" && event.JobID != job.ID {
			return errors.New("event job id does not match payload")
		}
		if err := job.Validate(); err != nil {
			return err
		}
		if event.Type == "job_created" {
			if _, exists := s.jobs[job.ID]; exists {
				return fmt.Errorf("duplicate creation of %s", job.ID)
			}
			if job.IdempotencyKey != "" {
				if _, exists := s.idempotency[job.IdempotencyKey]; exists {
					return fmt.Errorf("duplicate idempotency key %s", job.IdempotencyKey)
				}
				s.idempotency[job.IdempotencyKey] = IdempotencyRecord{JobID: job.ID, CreatedAt: job.CreatedAt}
			}
		} else if _, exists := s.jobs[job.ID]; !exists {
			return fmt.Errorf("update before creation of %s", job.ID)
		}
		s.jobs[job.ID] = model.CloneJob(&job)
	case "jobs_claimed", "jobs_recovered", "jobs_refreshed":
		var jobs []*model.Job
		if err := json.Unmarshal(event.Data, &jobs); err != nil {
			return err
		}
		for _, job := range jobs {
			if job == nil {
				return errors.New("batch event contains nil job")
			}
			if _, exists := s.jobs[job.ID]; !exists {
				return fmt.Errorf("batch update before creation of %s", job.ID)
			}
			if err := job.Validate(); err != nil {
				return err
			}
			s.jobs[job.ID] = model.CloneJob(job)
		}
	default:
		return fmt.Errorf("unknown event type %q", event.Type)
	}
	return nil
}

func Recover(cfg config.Config) (Recovery, error) {
	if err := cfg.Validate(); err != nil {
		return Recovery{}, err
	}
	if _, err := os.Stat(cfg.LockPath()); err == nil {
		return Recovery{}, errors.New("cannot recover while queue lock exists")
	}
	s := &Store{cfg: cfg, jobs: make(map[string]*model.Job), idempotency: make(map[string]IdempotencyRecord)}
	return s.recoverLocked()
}

func InspectSnapshot(path string) (Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var snapshot Snapshot
	if err := dec.Decode(&snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func JournalEvents(path string) ([]Event, error) {
	_, events, err := readAndVerifyJournal(path)
	return events, err
}

func AuditText(v Verification) string {
	var b strings.Builder
	fmt.Fprintf(&b, "valid: %t\n", v.Valid)
	fmt.Fprintf(&b, "events: %d\n", v.Events)
	fmt.Fprintf(&b, "first sequence: %d\n", v.FirstSequence)
	fmt.Fprintf(&b, "last sequence: %d\n", v.LastSequence)
	fmt.Fprintf(&b, "last hash: %s\n", v.LastHash)
	return b.String()
}

func SortedJobIDs(jobs map[string]*model.Job) []string {
	ids := make([]string, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func ParseSequence(value string) (uint64, error) {
	sequence, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid sequence %q: %w", value, err)
	}
	return sequence, nil
}

func CopyJournal(w io.Writer, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(w, file)
	return err
}
