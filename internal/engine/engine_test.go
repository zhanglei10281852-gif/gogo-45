package engine

import (
	"testing"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/model"
)

type fakeClock struct{ current time.Time }

func (f *fakeClock) Now() time.Time { return f.current }

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SnapshotEvery = 2
	cfg.HeartbeatGraceSeconds = 0
	return cfg
}

func TestLifecycleRetryAndComplete(t *testing.T) {
	clock := &fakeClock{current: time.Unix(1_700_000_000, 0).UTC()}
	queue, err := OpenWithClock(testConfig(t), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	result, err := queue.Enqueue(model.EnqueueRequest{ID: "job-a", Type: "email", Payload: []byte(`{"to":"a"}`), MaxAttempts: 2, Backoff: &model.BackoffPolicy{Kind: "fixed", BaseSeconds: 5, MaxSeconds: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.State != model.StateReady {
		t.Fatalf("state = %s", result.Job.State)
	}
	worker := model.Worker{ID: "w1", Queues: []string{"default"}, Capacity: model.Resources{CPU: 1, MemoryMB: 64, Slots: 1}}
	claimed, err := queue.Claim(model.ClaimRequest{Worker: worker})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs=%d err=%v", len(claimed), err)
	}
	failed, err := queue.Fail(model.FailRequest{JobID: "job-a", LeaseToken: claimed[0].Lease.Token, Code: "temporary", Message: "try later", Retryable: true})
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != model.StateRetryWait {
		t.Fatalf("state = %s", failed.State)
	}
	clock.current = clock.current.Add(5 * time.Second)
	claimed, err = queue.Claim(model.ClaimRequest{Worker: worker})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second claim: jobs=%d err=%v", len(claimed), err)
	}
	completed, err := queue.Complete(model.CompleteRequest{JobID: "job-a", LeaseToken: claimed[0].Lease.Token, Result: []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != model.StateSucceeded || completed.Attempts != 2 {
		t.Fatalf("completed=%+v", completed)
	}
}
