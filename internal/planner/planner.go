package planner

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"QueueForge/internal/model"
)

type DurationEstimate struct {
	JobType string `json:"job_type"`
	Seconds int64  `json:"seconds"`
}

type PlanRequest struct {
	Workers        []model.Worker     `json:"workers"`
	Durations      []DurationEstimate `json:"durations"`
	StartAt        time.Time          `json:"start_at"`
	HorizonSeconds int64              `json:"horizon_seconds"`
}

type Assignment struct {
	JobID    string    `json:"job_id"`
	WorkerID string    `json:"worker_id"`
	StartAt  time.Time `json:"start_at"`
	FinishAt time.Time `json:"finish_at"`
	Priority int       `json:"priority"`
}

type Unscheduled struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

type Plan struct {
	StartAt           time.Time        `json:"start_at"`
	EndAt             time.Time        `json:"end_at"`
	Assignments       []Assignment     `json:"assignments"`
	Unscheduled       []Unscheduled    `json:"unscheduled"`
	WorkerBusySeconds map[string]int64 `json:"worker_busy_seconds"`
	CriticalPath      []string         `json:"critical_path"`
}

type reservation struct {
	jobID     string
	startAt   time.Time
	finishAt  time.Time
	resources model.Resources
}

type workerSchedule struct {
	worker       model.Worker
	reservations []reservation
}

func Build(jobs []*model.Job, request PlanRequest) (Plan, error) {
	if len(request.Workers) == 0 {
		return Plan{}, errors.New("at least one worker is required")
	}
	if request.StartAt.IsZero() {
		request.StartAt = time.Now().UTC()
	}
	request.StartAt = request.StartAt.UTC()
	if request.HorizonSeconds == 0 {
		request.HorizonSeconds = 86400
	}
	if request.HorizonSeconds < 1 || request.HorizonSeconds > 365*86400 {
		return Plan{}, errors.New("horizon_seconds is outside valid range")
	}
	durations := make(map[string]time.Duration)
	for _, estimate := range request.Durations {
		if estimate.JobType == "" || estimate.Seconds < 1 {
			return Plan{}, errors.New("duration estimates require job_type and positive seconds")
		}
		if _, exists := durations[estimate.JobType]; exists {
			return Plan{}, fmt.Errorf("duplicate duration for type %s", estimate.JobType)
		}
		durations[estimate.JobType] = time.Duration(estimate.Seconds) * time.Second
	}
	schedules := make([]workerSchedule, 0, len(request.Workers))
	workerIDs := make(map[string]bool)
	for _, worker := range request.Workers {
		if err := worker.Validate(); err != nil {
			return Plan{}, fmt.Errorf("worker %s: %w", worker.ID, err)
		}
		if workerIDs[worker.ID] {
			return Plan{}, fmt.Errorf("duplicate worker %s", worker.ID)
		}
		workerIDs[worker.ID] = true
		schedules = append(schedules, workerSchedule{worker: worker})
	}
	byID := make(map[string]*model.Job, len(jobs))
	for _, job := range jobs {
		if job != nil {
			byID[job.ID] = job
		}
	}
	if err := model.ValidateDAG(byID); err != nil {
		return Plan{}, err
	}
	plan := Plan{StartAt: request.StartAt, EndAt: request.StartAt, WorkerBusySeconds: make(map[string]int64)}
	finishTimes := make(map[string]time.Time)
	remaining := make(map[string]*model.Job)
	for id, job := range byID {
		if !job.State.Terminal() && job.State != model.StateLeased {
			remaining[id] = job
		}
	}
	horizon := request.StartAt.Add(time.Duration(request.HorizonSeconds) * time.Second)
	for len(remaining) > 0 {
		ready := readyJobs(remaining, byID, finishTimes)
		if len(ready) == 0 {
			break
		}
		progress := false
		for _, job := range ready {
			duration, ok := durations[job.Type]
			if !ok {
				plan.Unscheduled = append(plan.Unscheduled, Unscheduled{JobID: job.ID, Reason: "no duration estimate for type " + job.Type})
				delete(remaining, job.ID)
				progress = true
				continue
			}
			index, start, reason := chooseWorker(schedules, job, request.StartAt, finishTimes, duration)
			if index < 0 {
				plan.Unscheduled = append(plan.Unscheduled, Unscheduled{JobID: job.ID, Reason: reason})
				delete(remaining, job.ID)
				progress = true
				continue
			}
			finish := start.Add(duration)
			if finish.After(horizon) {
				plan.Unscheduled = append(plan.Unscheduled, Unscheduled{JobID: job.ID, Reason: "completion exceeds planning horizon"})
				delete(remaining, job.ID)
				progress = true
				continue
			}
			assignment := Assignment{JobID: job.ID, WorkerID: schedules[index].worker.ID, StartAt: start, FinishAt: finish, Priority: job.Priority}
			plan.Assignments = append(plan.Assignments, assignment)
			plan.WorkerBusySeconds[assignment.WorkerID] += int64(duration.Seconds())
			schedules[index].reservations = append(schedules[index].reservations, reservation{jobID: job.ID, startAt: start, finishAt: finish, resources: job.Resources})
			finishTimes[job.ID] = finish
			if finish.After(plan.EndAt) {
				plan.EndAt = finish
			}
			delete(remaining, job.ID)
			progress = true
		}
		if !progress {
			break
		}
	}
	for id := range remaining {
		plan.Unscheduled = append(plan.Unscheduled, Unscheduled{JobID: id, Reason: "dependencies cannot be scheduled"})
	}
	sort.Slice(plan.Unscheduled, func(i, j int) bool { return plan.Unscheduled[i].JobID < plan.Unscheduled[j].JobID })
	plan.CriticalPath = criticalPath(byID, durations)
	return plan, nil
}
func readyJobs(remaining, all map[string]*model.Job, finish map[string]time.Time) []*model.Job {
	result := make([]*model.Job, 0)
	for _, job := range remaining {
		ready := true
		for _, dependency := range job.Dependencies {
			dep := all[dependency]
			if dep.State == model.StateSucceeded {
				continue
			}
			if _, planned := finish[dependency]; !planned {
				ready = false
				break
			}
		}
		if ready {
			result = append(result, job)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority > result[j].Priority
		}
		if result[i].AvailableAt.Equal(result[j].AvailableAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].AvailableAt.Before(result[j].AvailableAt)
	})
	return result
}

func chooseWorker(schedules []workerSchedule, job *model.Job, planStart time.Time, finish map[string]time.Time, duration time.Duration) (int, time.Time, string) {
	dependencyReady := planStart
	for _, dependency := range job.Dependencies {
		if value := finish[dependency]; value.After(dependencyReady) {
			dependencyReady = value
		}
	}
	if job.AvailableAt.After(dependencyReady) {
		dependencyReady = job.AvailableAt
	}
	best := -1
	var bestStart time.Time
	capacitySeen := false
	routeSeen := false
	for index := range schedules {
		schedule := &schedules[index]
		if !job.Resources.Fits(schedule.worker.Capacity) {
			continue
		}
		capacitySeen = true
		if !workerRoutes(schedule.worker, job) {
			continue
		}
		routeSeen = true
		start := earliestFeasibleStart(schedule, job.Resources, dependencyReady, duration)
		if best < 0 || start.Before(bestStart) || (start.Equal(bestStart) && schedule.worker.ID < schedules[best].worker.ID) {
			best, bestStart = index, start
		}
	}
	if best >= 0 {
		return best, bestStart, ""
	}
	if !capacitySeen {
		return -1, time.Time{}, "no worker has sufficient capacity"
	}
	if !routeSeen {
		return -1, time.Time{}, "no worker matches queue and labels"
	}
	return -1, time.Time{}, "no eligible worker"
}

func workerRoutes(worker model.Worker, job *model.Job) bool {
	queueOK := false
	for _, queue := range worker.Queues {
		if queue == "*" || queue == job.Queue {
			queueOK = true
			break
		}
	}
	if !queueOK {
		return false
	}
	for key, value := range job.RequiredLabels {
		if worker.Labels[key] != value {
			return false
		}
	}
	return true
}

func earliestFeasibleStart(schedule *workerSchedule, resources model.Resources, notBefore time.Time, duration time.Duration) time.Time {
	candidates := []time.Time{notBefore}
	for _, existing := range schedule.reservations {
		if existing.finishAt.After(notBefore) {
			candidates = append(candidates, existing.finishAt)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Before(candidates[j]) })
	var previous time.Time
	for _, start := range candidates {
		if !previous.IsZero() && start.Equal(previous) {
			continue
		}
		previous = start
		if intervalFits(schedule.reservations, resources, schedule.worker.Capacity, start, start.Add(duration)) {
			return start
		}
	}
	panic("no feasible start after all reservations")
}

func intervalFits(reservations []reservation, requested, capacity model.Resources, start, finish time.Time) bool {
	boundaries := []time.Time{start, finish}
	for _, existing := range reservations {
		if !existing.startAt.Before(finish) || !start.Before(existing.finishAt) {
			continue
		}
		if existing.startAt.After(start) && existing.startAt.Before(finish) {
			boundaries = append(boundaries, existing.startAt)
		}
		if existing.finishAt.After(start) && existing.finishAt.Before(finish) {
			boundaries = append(boundaries, existing.finishAt)
		}
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Before(boundaries[j]) })
	for index := 0; index+1 < len(boundaries); index++ {
		at, next := boundaries[index], boundaries[index+1]
		if !at.Before(next) {
			continue
		}
		used := model.Resources{}
		for _, existing := range reservations {
			if !existing.startAt.After(at) && at.Before(existing.finishAt) {
				used = used.Add(existing.resources)
			}
		}
		remaining := capacity.Sub(used)
		if !requested.Fits(remaining) {
			return false
		}
	}
	return true
}

func criticalPath(jobs map[string]*model.Job, durations map[string]time.Duration) []string {
	type pathResult struct {
		duration time.Duration
		ids      []string
	}
	memo := make(map[string]pathResult)
	var visit func(string) pathResult
	visit = func(id string) pathResult {
		if result, ok := memo[id]; ok {
			return result
		}
		job := jobs[id]
		best := pathResult{}
		for _, dependency := range job.Dependencies {
			candidate := visit(dependency)
			if candidate.duration > best.duration {
				best = candidate
			}
		}
		result := pathResult{duration: best.duration + durations[job.Type], ids: append(append([]string(nil), best.ids...), id)}
		memo[id] = result
		return result
	}
	best := pathResult{}
	ids := make([]string, 0, len(jobs))
	for id := range jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		candidate := visit(id)
		if candidate.duration > best.duration {
			best = candidate
		}
	}
	return best.ids
}
