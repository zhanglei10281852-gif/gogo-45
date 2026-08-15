package planner

import (
	"testing"
	"time"

	"QueueForge/internal/model"
)

func TestBuildHonorsDependencies(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	job := func(id string, deps ...string) *model.Job {
		return &model.Job{ID: id, Queue: "q", Type: "work", State: model.StateBlocked, CreatedAt: now, UpdatedAt: now, AvailableAt: now, Dependencies: deps, MaxAttempts: 1, Backoff: model.BackoffPolicy{Kind: "fixed"}, Resources: model.Resources{Slots: 1}, Payload: []byte(`null`), History: []model.Transition{{To: model.StateBlocked, At: now, Reason: "test"}}}
	}
	worker := model.Worker{ID: "w", Queues: []string{"q"}, Capacity: model.Resources{Slots: 1}}
	plan, err := Build([]*model.Job{job("a"), job("b", "a")}, PlanRequest{Workers: []model.Worker{worker}, Durations: []DurationEstimate{{JobType: "work", Seconds: 10}}, StartAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Assignments) != 2 {
		t.Fatalf("assignments=%d", len(plan.Assignments))
	}
	if plan.Assignments[1].StartAt.Before(plan.Assignments[0].FinishAt) {
		t.Fatal("dependent job overlaps dependency")
	}
}

func TestBuildReservesAggregateWorkerResourcesAcrossSlots(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	job := func(id string, resources model.Resources) *model.Job {
		return &model.Job{ID: id, Queue: "q", Type: "work", State: model.StateReady, CreatedAt: start, UpdatedAt: start, AvailableAt: start, Resources: resources}
	}
	jobs := []*model.Job{
		job("c", model.Resources{CPU: 1, MemoryMB: 6, Slots: 1}),
		job("b", model.Resources{CPU: 2, MemoryMB: 2, Slots: 1}),
		job("a", model.Resources{CPU: 3, MemoryMB: 2, Slots: 1}),
	}
	worker := model.Worker{ID: "w", Queues: []string{"q"}, Capacity: model.Resources{CPU: 4, MemoryMB: 8, Slots: 3}}
	plan, err := Build(jobs, PlanRequest{Workers: []model.Worker{worker}, Durations: []DurationEstimate{{JobType: "work", Seconds: 10}}, StartAt: start, HorizonSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Assignments) != 3 {
		t.Fatalf("assignments=%+v", plan.Assignments)
	}
	wantStarts := map[string]time.Time{"a": start, "b": start.Add(10 * time.Second), "c": start}
	for _, assignment := range plan.Assignments {
		if want := wantStarts[assignment.JobID]; !assignment.StartAt.Equal(want) {
			t.Errorf("%s starts at %s, want %s", assignment.JobID, assignment.StartAt, want)
		}
	}
}
