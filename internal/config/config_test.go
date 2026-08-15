package config

import (
	"strings"
	"testing"
)

func TestDecodeStrictRejectsUnknownField(t *testing.T) {
	var cfg Config
	err := DecodeStrict(strings.NewReader(`{"data_dir":"x","surprise":true}`), &cfg)
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestDefaultValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}
