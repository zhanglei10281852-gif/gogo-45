package report

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"QueueForge/internal/model"
)

type Summary struct {
	GeneratedAt        time.Time               `json:"generated_at"`
	Total              int                     `json:"total"`
	ByState            map[model.State]int     `json:"by_state"`
	ByQueue            map[string]QueueSummary `json:"by_queue"`
	ReadyOldestSeconds int64                   `json:"ready_oldest_seconds"`
	Retrying           int                     `json:"retrying"`
	Leased             int                     `json:"leased"`
	Succeeded          int                     `json:"succeeded"`
	Dead               int                     `json:"dead"`
	TotalAttempts      int                     `json:"total_attempts"`
	AverageAttempts    float64                 `json:"average_attempts"`
	SuccessRate        float64                 `json:"success_rate"`
}

type QueueSummary struct {
	Total       int `json:"total"`
	Ready       int `json:"ready"`
	Leased      int `json:"leased"`
	Blocked     int `json:"blocked"`
	Dead        int `json:"dead"`
	Succeeded   int `json:"succeeded"`
	MaxPriority int `json:"max_priority"`
}

func Build(jobs []*model.Job, now time.Time) Summary {
	s := Summary{GeneratedAt: now.UTC(), ByState: make(map[model.State]int), ByQueue: make(map[string]QueueSummary)}
	var oldest time.Time
	for _, j := range jobs {
		s.Total++
		s.ByState[j.State]++
		qs := s.ByQueue[j.Queue]
		qs.Total++
		switch j.State {
		case model.StateReady:
			qs.Ready++
			if oldest.IsZero() || j.AvailableAt.Before(oldest) {
				oldest = j.AvailableAt
			}
		case model.StateLeased:
			qs.Leased++
			s.Leased++
		case model.StateBlocked:
			qs.Blocked++
		case model.StateDead:
			qs.Dead++
			s.Dead++
		case model.StateSucceeded:
			qs.Succeeded++
			s.Succeeded++
		case model.StateRetryWait:
			s.Retrying++
		}
		if j.Priority > qs.MaxPriority || qs.Total == 1 {
			qs.MaxPriority = j.Priority
		}
		s.ByQueue[j.Queue] = qs
		s.TotalAttempts += j.Attempts
	}
	if !oldest.IsZero() && now.After(oldest) {
		s.ReadyOldestSeconds = int64(now.Sub(oldest).Seconds())
	}
	if s.Total > 0 {
		s.AverageAttempts = float64(s.TotalAttempts) / float64(s.Total)
	}
	terminal := s.Succeeded + s.Dead
	if terminal > 0 {
		s.SuccessRate = float64(s.Succeeded) / float64(terminal)
	}
	return s
}

func Text(summary Summary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "QueueForge report at %s\n", summary.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "jobs: %d  ready: %d  leased: %d  retrying: %d  succeeded: %d  dead: %d\n",
		summary.Total, summary.ByState[model.StateReady], summary.Leased, summary.Retrying, summary.Succeeded, summary.Dead)
	fmt.Fprintf(&b, "attempts: %d  average: %.2f  terminal success rate: %.2f%%\n", summary.TotalAttempts, summary.AverageAttempts, summary.SuccessRate*100)
	fmt.Fprintf(&b, "oldest ready age: %ds\n", summary.ReadyOldestSeconds)
	queues := make([]string, 0, len(summary.ByQueue))
	for queue := range summary.ByQueue {
		queues = append(queues, queue)
	}
	sort.Strings(queues)
	if len(queues) > 0 {
		b.WriteString("queues:\n")
	}
	for _, queue := range queues {
		q := summary.ByQueue[queue]
		fmt.Fprintf(&b, "  %-20s total=%d ready=%d leased=%d blocked=%d dead=%d succeeded=%d max_priority=%d\n",
			queue, q.Total, q.Ready, q.Leased, q.Blocked, q.Dead, q.Succeeded, q.MaxPriority)
	}
	return b.String()
}

type TimelineEntry struct {
	JobID       string      `json:"job_id"`
	Queue       string      `json:"queue"`
	Type        string      `json:"type"`
	State       model.State `json:"state"`
	AvailableAt time.Time   `json:"available_at"`
	Deadline    *time.Time  `json:"deadline,omitempty"`
	Priority    int         `json:"priority"`
	Attempts    int         `json:"attempts"`
	MaxAttempts int         `json:"max_attempts"`
}

func Timeline(jobs []*model.Job) []TimelineEntry {
	entries := make([]TimelineEntry, 0, len(jobs))
	for _, j := range jobs {
		if j.State.Terminal() {
			continue
		}
		entries = append(entries, TimelineEntry{JobID: j.ID, Queue: j.Queue, Type: j.Type, State: j.State, AvailableAt: j.AvailableAt, Deadline: j.Deadline, Priority: j.Priority, Attempts: j.Attempts, MaxAttempts: j.MaxAttempts})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].AvailableAt.Equal(entries[j].AvailableAt) {
			if entries[i].Priority == entries[j].Priority {
				return entries[i].JobID < entries[j].JobID
			}
			return entries[i].Priority > entries[j].Priority
		}
		return entries[i].AvailableAt.Before(entries[j].AvailableAt)
	})
	return entries
}
