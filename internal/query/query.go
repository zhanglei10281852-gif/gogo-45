package query

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"QueueForge/internal/model"
)

type Filter struct {
	States          []model.State     `json:"states,omitempty"`
	Queues          []string          `json:"queues,omitempty"`
	Types           []string          `json:"types,omitempty"`
	MinPriority     *int              `json:"min_priority,omitempty"`
	MaxPriority     *int              `json:"max_priority,omitempty"`
	CreatedAfter    *time.Time        `json:"created_after,omitempty"`
	CreatedBefore   *time.Time        `json:"created_before,omitempty"`
	AvailableBefore *time.Time        `json:"available_before,omitempty"`
	WorkerID        string            `json:"worker_id,omitempty"`
	HasError        *bool             `json:"has_error,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Dependency      string            `json:"dependency,omitempty"`
	Search          string            `json:"search,omitempty"`
}

type SortOrder string

const (
	SortCreatedAsc  SortOrder = "created_asc"
	SortCreatedDesc SortOrder = "created_desc"
	SortPriority    SortOrder = "priority"
	SortAvailable   SortOrder = "available"
	SortUpdatedDesc SortOrder = "updated_desc"
)

type PageRequest struct {
	Filter Filter    `json:"filter"`
	Sort   SortOrder `json:"sort"`
	Offset int       `json:"offset"`
	Limit  int       `json:"limit"`
}
type Page struct {
	Jobs    []*model.Job `json:"jobs"`
	Total   int          `json:"total"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
	HasMore bool         `json:"has_more"`
}

func (f Filter) Validate() error {
	var problems []string
	for _, state := range f.States {
		if !state.Valid() {
			problems = append(problems, fmt.Sprintf("invalid state %q", state))
		}
	}
	for _, queue := range f.Queues {
		if strings.TrimSpace(queue) == "" {
			problems = append(problems, "queue filter cannot be empty")
		}
	}
	for _, kind := range f.Types {
		if strings.TrimSpace(kind) == "" {
			problems = append(problems, "type filter cannot be empty")
		}
	}
	if f.MinPriority != nil && f.MaxPriority != nil && *f.MinPriority > *f.MaxPriority {
		problems = append(problems, "min_priority exceeds max_priority")
	}
	if f.CreatedAfter != nil && f.CreatedBefore != nil && f.CreatedAfter.After(*f.CreatedBefore) {
		problems = append(problems, "created_after exceeds created_before")
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func Select(jobs []*model.Job, request PageRequest) (Page, error) {
	if err := request.Filter.Validate(); err != nil {
		return Page{}, err
	}
	if request.Offset < 0 {
		return Page{}, errors.New("offset must be nonnegative")
	}
	if request.Limit == 0 {
		request.Limit = 100
	}
	if request.Limit < 1 || request.Limit > 10000 {
		return Page{}, errors.New("limit must be between 1 and 10000")
	}
	if request.Sort == "" {
		request.Sort = SortCreatedAsc
	}
	if !validSort(request.Sort) {
		return Page{}, fmt.Errorf("invalid sort %q", request.Sort)
	}
	selected := make([]*model.Job, 0)
	for _, job := range jobs {
		if matches(job, request.Filter) {
			selected = append(selected, model.CloneJob(job))
		}
	}
	sortJobs(selected, request.Sort)
	page := Page{Total: len(selected), Offset: request.Offset, Limit: request.Limit}
	if request.Offset >= len(selected) {
		page.Jobs = []*model.Job{}
		return page, nil
	}
	end := request.Offset + request.Limit
	if end > len(selected) {
		end = len(selected)
	}
	page.Jobs = selected[request.Offset:end]
	page.HasMore = end < len(selected)
	return page, nil
}

func validSort(order SortOrder) bool {
	switch order {
	case SortCreatedAsc, SortCreatedDesc, SortPriority, SortAvailable, SortUpdatedDesc:
		return true
	default:
		return false
	}
}

func matches(job *model.Job, filter Filter) bool {
	if job == nil {
		return false
	}
	if len(filter.States) > 0 && !containsState(filter.States, job.State) {
		return false
	}
	if len(filter.Queues) > 0 && !containsString(filter.Queues, job.Queue) {
		return false
	}
	if len(filter.Types) > 0 && !containsString(filter.Types, job.Type) {
		return false
	}
	if filter.MinPriority != nil && job.Priority < *filter.MinPriority {
		return false
	}
	if filter.MaxPriority != nil && job.Priority > *filter.MaxPriority {
		return false
	}
	if filter.CreatedAfter != nil && job.CreatedAt.Before(*filter.CreatedAfter) {
		return false
	}
	if filter.CreatedBefore != nil && job.CreatedAt.After(*filter.CreatedBefore) {
		return false
	}
	if filter.AvailableBefore != nil && job.AvailableAt.After(*filter.AvailableBefore) {
		return false
	}
	if filter.WorkerID != "" && (job.Lease == nil || job.Lease.WorkerID != filter.WorkerID) {
		return false
	}
	if filter.HasError != nil && (job.LastError != nil) != *filter.HasError {
		return false
	}
	if !containsPairs(job.Metadata, filter.Metadata) {
		return false
	}
	if !containsPairs(job.RequiredLabels, filter.Labels) {
		return false
	}
	if filter.Dependency != "" && !containsString(job.Dependencies, filter.Dependency) {
		return false
	}
	if filter.Search != "" {
		needle := strings.ToLower(filter.Search)
		haystack := strings.ToLower(job.ID + "\x00" + job.Queue + "\x00" + job.Type)
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}
func containsState(values []model.State, wanted model.State) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsPairs(values, required map[string]string) bool {
	for key, value := range required {
		if values[key] != value {
			return false
		}
	}
	return true
}

func sortJobs(jobs []*model.Job, order SortOrder) {
	sort.SliceStable(jobs, func(i, j int) bool {
		left, right := jobs[i], jobs[j]
		switch order {
		case SortCreatedDesc:
			if left.CreatedAt.Equal(right.CreatedAt) {
				return left.ID < right.ID
			}
			return left.CreatedAt.After(right.CreatedAt)
		case SortPriority:
			if left.Priority != right.Priority {
				return left.Priority > right.Priority
			}
			if !left.AvailableAt.Equal(right.AvailableAt) {
				return left.AvailableAt.Before(right.AvailableAt)
			}
		case SortAvailable:
			if !left.AvailableAt.Equal(right.AvailableAt) {
				return left.AvailableAt.Before(right.AvailableAt)
			}
			if left.Priority != right.Priority {
				return left.Priority > right.Priority
			}
		case SortUpdatedDesc:
			if !left.UpdatedAt.Equal(right.UpdatedAt) {
				return left.UpdatedAt.After(right.UpdatedAt)
			}
		default:
			if !left.CreatedAt.Equal(right.CreatedAt) {
				return left.CreatedAt.Before(right.CreatedAt)
			}
		}
		return left.ID < right.ID
	})
}

type GraphNode struct {
	JobID        string      `json:"job_id"`
	State        model.State `json:"state"`
	Dependencies []string    `json:"dependencies"`
	Dependents   []string    `json:"dependents"`
	Depth        int         `json:"depth"`
	Blocking     int         `json:"blocking"`
}

func DependencyGraph(jobs []*model.Job) ([]GraphNode, error) {
	byID := make(map[string]*model.Job, len(jobs))
	dependents := make(map[string][]string)
	for _, job := range jobs {
		if job == nil {
			return nil, errors.New("nil job in graph")
		}
		if _, exists := byID[job.ID]; exists {
			return nil, fmt.Errorf("duplicate job %s", job.ID)
		}
		byID[job.ID] = job
		for _, dependency := range job.Dependencies {
			dependents[dependency] = append(dependents[dependency], job.ID)
		}
	}
	if err := model.ValidateDAG(byID); err != nil {
		return nil, err
	}
	depthMemo := make(map[string]int)
	var depth func(string) int
	depth = func(id string) int {
		if value, ok := depthMemo[id]; ok {
			return value
		}
		value := 0
		for _, dependency := range byID[id].Dependencies {
			candidate := depth(dependency) + 1
			if candidate > value {
				value = candidate
			}
		}
		depthMemo[id] = value
		return value
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]GraphNode, 0, len(ids))
	for _, id := range ids {
		job := byID[id]
		deps := append([]string(nil), job.Dependencies...)
		reverse := append([]string(nil), dependents[id]...)
		sort.Strings(deps)
		sort.Strings(reverse)
		blocking := 0
		for _, dep := range deps {
			if byID[dep].State != model.StateSucceeded {
				blocking++
			}
		}
		nodes = append(nodes, GraphNode{JobID: id, State: job.State, Dependencies: deps, Dependents: reverse, Depth: depth(id), Blocking: blocking})
	}
	return nodes, nil
}

type RoutingResult struct {
	WorkerID string            `json:"worker_id"`
	Eligible []string          `json:"eligible"`
	Rejected map[string]string `json:"rejected"`
}

func Route(jobs []*model.Job, worker model.Worker, now time.Time) (RoutingResult, error) {
	if err := worker.Validate(); err != nil {
		return RoutingResult{}, err
	}
	result := RoutingResult{WorkerID: worker.ID, Rejected: make(map[string]string)}
	remaining := worker.Capacity
	ordered := append([]*model.Job(nil), jobs...)
	sortJobs(ordered, SortPriority)
	for _, job := range ordered {
		reason := rejectionReason(job, worker, remaining, now)
		if reason != "" {
			result.Rejected[job.ID] = reason
			continue
		}
		result.Eligible = append(result.Eligible, job.ID)
		remaining = remaining.Sub(job.Resources)
	}
	return result, nil
}

func rejectionReason(job *model.Job, worker model.Worker, remaining model.Resources, now time.Time) string {
	if job == nil {
		return "nil job"
	}
	if job.State != model.StateReady {
		return "state is " + string(job.State)
	}
	if job.AvailableAt.After(now) {
		return "delay has not elapsed"
	}
	if job.Deadline != nil && !job.Deadline.After(now) {
		return "deadline elapsed"
	}
	queueOK := false
	for _, queue := range worker.Queues {
		if queue == "*" || queue == job.Queue {
			queueOK = true
			break
		}
	}
	if !queueOK {
		return "queue not subscribed"
	}
	for key, value := range job.RequiredLabels {
		if worker.Labels[key] != value {
			return "label mismatch: " + key
		}
	}
	if !job.Resources.Fits(remaining) {
		return "insufficient remaining capacity"
	}
	return ""
}
