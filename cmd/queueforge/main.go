package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"QueueForge/internal/config"
	"QueueForge/internal/engine"
	"QueueForge/internal/model"
	"QueueForge/internal/report"
	"QueueForge/internal/store"
)

const version = "1.0.0"

type application struct {
	stdout io.Writer
	stderr io.Writer
	stdin  io.Reader
	now    func() time.Time
}

func main() {
	app := application{stdout: os.Stdout, stderr: os.Stderr, stdin: os.Stdin, now: time.Now}
	if err := app.run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "queueforge:", err)
		os.Exit(1)
	}
}

func (a application) run(args []string) error {
	if len(args) == 0 {
		return a.usageError("command required")
	}
	if args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		a.printUsage()
		return nil
	}
	if args[0] == "version" || args[0] == "--version" {
		fmt.Fprintln(a.stdout, version)
		return nil
	}
	switch args[0] {
	case "validate":
		return a.validate(args[1:])
	case "enqueue":
		return a.enqueue(args[1:])
	case "claim":
		return a.claim(args[1:])
	case "heartbeat":
		return a.heartbeat(args[1:])
	case "complete":
		return a.complete(args[1:])
	case "fail":
		return a.fail(args[1:])
	case "recover":
		return a.recover(args[1:])
	case "report":
		return a.report(args[1:])
	default:
		return a.usageError("unknown command %q", args[0])
	}
}
func (a application) printUsage() {
	fmt.Fprintln(a.stdout, `QueueForge - durable offline background job queue

Usage:
  queueforge <command> [options]

Commands:
  validate   validate configuration, input, graph, and journal integrity
  enqueue    enqueue a job from strict JSON
  claim      lease eligible jobs to a worker
  heartbeat  extend an active lease
  complete   complete a leased job
  fail       fail a leased job, scheduling retry or dead-letter
  recover    verify journal and rebuild queue state
  report     print queue metrics, jobs, or timeline

Common options:
  -config PATH   strict JSON configuration file (defaults to built-in config)

JSON input is read from -input PATH, or stdin when PATH is "-".`)
}

func (a application) usageError(format string, values ...any) error {
	a.printUsage()
	return fmt.Errorf(format, values...)
}

type commonFlags struct {
	configPath string
	inputPath  string
	jsonOutput bool
}

func bindCommon(fs *flag.FlagSet, withInput bool) *commonFlags {
	common := &commonFlags{}
	fs.StringVar(&common.configPath, "config", "", "configuration JSON path")
	if withInput {
		fs.StringVar(&common.inputPath, "input", "-", "input JSON path or - for stdin")
	}
	fs.BoolVar(&common.jsonOutput, "json", true, "emit JSON output")
	return common
}

func parse(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	return nil
}

func (a application) loadConfig(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return config.Config{}, err
	}
	if path == "" {
		if env := os.Getenv("QUEUEFORGE_DATA_DIR"); env != "" {
			cfg.DataDir = filepath.Clean(env)
		}
	}
	return cfg, cfg.Validate()
}

func (a application) decodeInput(path string, target any) error {
	var reader io.Reader = a.stdin
	var file *os.File
	if path != "" && path != "-" {
		opened, err := os.Open(path)
		if err != nil {
			return err
		}
		file = opened
		defer file.Close()
		reader = file
	}
	if err := config.DecodeStrict(reader, target); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	return nil
}

func (a application) output(value any) error {
	enc := json.NewEncoder(a.stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(value)
}

func withQueue(cfg config.Config, action func(*engine.Queue) error) error {
	queue, err := engine.Open(cfg)
	if err != nil {
		return err
	}
	actionErr := action(queue)
	closeErr := queue.Close()
	return errors.Join(actionErr, closeErr)
}
func (a application) validate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	common := bindCommon(fs, false)
	input := fs.String("input", "", "optional enqueue request to validate")
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	result := struct {
		Config  bool                `json:"config"`
		Input   bool                `json:"input,omitempty"`
		Journal *store.Verification `json:"journal,omitempty"`
		Jobs    int                 `json:"jobs"`
	}{Config: true}
	if *input != "" {
		var request model.EnqueueRequest
		if err := a.decodeInput(*input, &request); err != nil {
			return err
		}
		result.Input = true
		if request.Type == "" {
			return errors.New("input type is required")
		}
		if len(request.Payload) > 0 && !json.Valid(request.Payload) {
			return errors.New("input payload is invalid JSON")
		}
	}
	if _, err := os.Stat(cfg.JournalPath()); err == nil {
		verification, err := store.Verify(cfg.JournalPath())
		if err != nil {
			return err
		}
		result.Journal = &verification
	}
	if _, err := os.Stat(cfg.LockPath()); err == nil {
		return errors.New("queue is currently locked; cannot perform full graph validation")
	}
	recovery, err := store.Recover(cfg)
	if err != nil {
		return err
	}
	result.Jobs = recovery.JobsRecovered
	return a.output(result)
}

func (a application) enqueue(args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	common := bindCommon(fs, true)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	var request model.EnqueueRequest
	if err := a.decodeInput(common.inputPath, &request); err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		result, err := queue.Enqueue(request)
		if err != nil {
			return err
		}
		return a.output(result)
	})
}

func (a application) claim(args []string) error {
	fs := flag.NewFlagSet("claim", flag.ContinueOnError)
	common := bindCommon(fs, true)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	var request model.ClaimRequest
	if err := a.decodeInput(common.inputPath, &request); err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		jobs, err := queue.Claim(request)
		if err != nil {
			return err
		}
		return a.output(struct {
			Jobs  []*model.Job `json:"jobs"`
			Count int          `json:"count"`
		}{jobs, len(jobs)})
	})
}

func (a application) heartbeat(args []string) error {
	fs := flag.NewFlagSet("heartbeat", flag.ContinueOnError)
	common := bindCommon(fs, true)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	var request model.HeartbeatRequest
	if err := a.decodeInput(common.inputPath, &request); err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		job, err := queue.Heartbeat(request)
		if err != nil {
			return err
		}
		return a.output(job)
	})
}
func (a application) complete(args []string) error {
	fs := flag.NewFlagSet("complete", flag.ContinueOnError)
	common := bindCommon(fs, true)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	var request model.CompleteRequest
	if err := a.decodeInput(common.inputPath, &request); err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		job, err := queue.Complete(request)
		if err != nil {
			return err
		}
		return a.output(job)
	})
}

func (a application) fail(args []string) error {
	fs := flag.NewFlagSet("fail", flag.ContinueOnError)
	common := bindCommon(fs, true)
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	var request model.FailRequest
	if err := a.decodeInput(common.inputPath, &request); err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		job, err := queue.Fail(request)
		if err != nil {
			return err
		}
		return a.output(job)
	})
}

func (a application) recover(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	common := bindCommon(fs, false)
	snapshot := fs.Bool("snapshot", true, "write a fresh verified snapshot")
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(cfg.LockPath()); err == nil {
		return errors.New("queue is locked by another process")
	}
	recovery, err := store.Recover(cfg)
	if err != nil {
		return err
	}
	if *snapshot {
		if err := withQueue(cfg, func(queue *engine.Queue) error { return queue.Snapshot() }); err != nil {
			return err
		}
	}
	return a.output(recovery)
}

func (a application) report(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	common := bindCommon(fs, false)
	format := fs.String("format", "text", "text, json, jobs, or timeline")
	if err := parse(fs, args); err != nil {
		return err
	}
	cfg, err := a.loadConfig(common.configPath)
	if err != nil {
		return err
	}
	return withQueue(cfg, func(queue *engine.Queue) error {
		if _, err := queue.Refresh(); err != nil {
			return err
		}
		jobs := queue.Jobs()
		summary := report.Build(jobs, a.now().UTC())
		switch *format {
		case "text":
			_, err = fmt.Fprint(a.stdout, report.Text(summary))
			return err
		case "json":
			return a.output(summary)
		case "jobs":
			return a.output(jobs)
		case "timeline":
			return a.output(report.Timeline(jobs))
		default:
			return fmt.Errorf("unknown report format %q", *format)
		}
	})
}

func decodeJSONBytes(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("unexpected additional JSON")
	}
	return nil
}
