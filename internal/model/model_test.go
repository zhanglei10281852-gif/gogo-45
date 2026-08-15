package model

import (
	"strings"
	"testing"
	"time"
)

func testJob(id string, dependencies ...string) *Job {
	now := time.Unix(100, 0).UTC()
	return &Job{
		ID: id, Queue: "default", Type: "test", Payload: []byte(`{}`),
		State: StateBlocked, CreatedAt: now, UpdatedAt: now, AvailableAt: now,
		Dependencies: dependencies, MaxAttempts: 3,
		Backoff:   BackoffPolicy{Kind: "fixed", BaseSeconds: 1, MaxSeconds: 1},
		Resources: Resources{Slots: 1},
		History:   []Transition{{To: StateBlocked, At: now, Reason: "test"}},
	}
}

func TestValidateDAGRejectsCycle(t *testing.T) {
	jobs := map[string]*Job{
		"a": testJob("a", "b"),
		"b": testJob("b", "c"),
		"c": testJob("c", "a"),
	}
	err := ValidateDAG(jobs)
	if err == nil || !strings.Contains(err.Error(), "dependency cycle") {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestTransitionStateMachine(t *testing.T) {
	job := testJob("a")
	now := time.Unix(200, 0).UTC()
	if err := job.Transition(StateReady, now, "unblocked", "scheduler"); err != nil {
		t.Fatal(err)
	}
	if err := job.Transition(StateSucceeded, now, "skip lease", "test"); err == nil {
		t.Fatal("expected illegal transition")
	}
	if job.State != StateReady {
		t.Fatalf("illegal transition changed state to %s", job.State)
	}
}
