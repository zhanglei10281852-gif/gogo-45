package store

import (
	"os"
	"testing"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/model"
)

func storeConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SnapshotEvery = 100
	return cfg
}

func validJob() *model.Job {
	now := time.Unix(100, 0).UTC()
	return &model.Job{ID: "one", Queue: "default", Type: "test", Payload: []byte(`null`), State: model.StateReady, CreatedAt: now, UpdatedAt: now, AvailableAt: now, MaxAttempts: 1, Backoff: model.BackoffPolicy{Kind: "fixed", BaseSeconds: 0, MaxSeconds: 0}, Resources: model.Resources{Slots: 1}, History: []model.Transition{{To: model.StateReady, At: now, Reason: "created"}}}
}

func TestJournalTamperDetected(t *testing.T) {
	cfg := storeConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(validJob()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.JournalPath())
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if data[i] == 'o' {
			data[i] = 'x'
			break
		}
	}
	if err := os.WriteFile(cfg.JournalPath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(cfg.JournalPath()); err == nil {
		t.Fatal("expected tamper detection")
	}
}
