package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	DataDir                   string   `json:"data_dir"`
	LeaseSeconds              int64    `json:"lease_seconds"`
	HeartbeatGraceSeconds     int64    `json:"heartbeat_grace_seconds"`
	SnapshotEvery             int      `json:"snapshot_every"`
	MaxJournalBytes           int64    `json:"max_journal_bytes"`
	DefaultMaxAttempts        int      `json:"default_max_attempts"`
	DefaultBackoffSeconds     int64    `json:"default_backoff_seconds"`
	DefaultBackoffMaxSeconds  int64    `json:"default_backoff_max_seconds"`
	DefaultQueues             []string `json:"default_queues"`
	ClockSkewToleranceSeconds int64    `json:"clock_skew_tolerance_seconds"`
	IdempotencyRetentionHours int64    `json:"idempotency_retention_hours"`
}

func Default() Config {
	return Config{
		DataDir: ".queueforge", LeaseSeconds: 60,
		HeartbeatGraceSeconds: 15, SnapshotEvery: 100,
		MaxJournalBytes: 64 << 20, DefaultMaxAttempts: 3,
		DefaultBackoffSeconds: 5, DefaultBackoffMaxSeconds: 3600,
		DefaultQueues: []string{"default"}, ClockSkewToleranceSeconds: 5,
		IdempotencyRetentionHours: 168,
	}
}
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, cfg.Validate()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := decodeStrict(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if !filepath.IsAbs(cfg.DataDir) {
		cfg.DataDir = filepath.Join(filepath.Dir(path), cfg.DataDir)
	}
	cfg.DataDir = filepath.Clean(cfg.DataDir)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func decodeStrict(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func DecodeStrict(reader io.Reader, target any) error {
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func (c Config) Validate() error {
	var problems []string
	if strings.TrimSpace(c.DataDir) == "" {
		problems = append(problems, "data_dir is required")
	}
	if c.LeaseSeconds < 1 || c.LeaseSeconds > 86400 {
		problems = append(problems, "lease_seconds must be between 1 and 86400")
	}
	if c.HeartbeatGraceSeconds < 0 || c.HeartbeatGraceSeconds > c.LeaseSeconds {
		problems = append(problems, "heartbeat_grace_seconds must be between 0 and lease_seconds")
	}
	if c.SnapshotEvery < 1 || c.SnapshotEvery > 1_000_000 {
		problems = append(problems, "snapshot_every must be between 1 and 1000000")
	}
	if c.MaxJournalBytes < 1024 || c.MaxJournalBytes > 1<<40 {
		problems = append(problems, "max_journal_bytes must be between 1024 and 1 TiB")
	}
	if c.DefaultMaxAttempts < 1 || c.DefaultMaxAttempts > 1000 {
		problems = append(problems, "default_max_attempts must be between 1 and 1000")
	}
	if c.DefaultBackoffSeconds < 0 {
		problems = append(problems, "default_backoff_seconds must be nonnegative")
	}
	if c.DefaultBackoffMaxSeconds < c.DefaultBackoffSeconds {
		problems = append(problems, "default_backoff_max_seconds must be at least default_backoff_seconds")
	}
	if len(c.DefaultQueues) == 0 {
		problems = append(problems, "default_queues must not be empty")
	}
	seen := make(map[string]bool)
	for _, queue := range c.DefaultQueues {
		if strings.TrimSpace(queue) == "" {
			problems = append(problems, "default queue cannot be empty")
		}
		if seen[queue] {
			problems = append(problems, "duplicate default queue "+queue)
		}
		seen[queue] = true
	}
	if c.ClockSkewToleranceSeconds < 0 || c.ClockSkewToleranceSeconds > 3600 {
		problems = append(problems, "clock_skew_tolerance_seconds must be between 0 and 3600")
	}
	if c.IdempotencyRetentionHours < 1 || c.IdempotencyRetentionHours > 87600 {
		problems = append(problems, "idempotency_retention_hours must be between 1 and 87600")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (c Config) LeaseDuration() time.Duration { return time.Duration(c.LeaseSeconds) * time.Second }
func (c Config) HeartbeatGrace() time.Duration {
	return time.Duration(c.HeartbeatGraceSeconds) * time.Second
}
func (c Config) IdempotencyRetention() time.Duration {
	return time.Duration(c.IdempotencyRetentionHours) * time.Hour
}
func (c Config) JournalPath() string  { return filepath.Join(c.DataDir, "journal.jsonl") }
func (c Config) SnapshotPath() string { return filepath.Join(c.DataDir, "snapshot.json") }
func (c Config) LockPath() string     { return filepath.Join(c.DataDir, "queueforge.lock") }

func WriteDefault(path string) error {
	cfg := Default()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}
