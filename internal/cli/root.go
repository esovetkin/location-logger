package cli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"location_logger/internal/daemon"
	exporter "location_logger/internal/export"
	"location_logger/internal/paths"
)

func Run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}

	switch args[0] {
	case "daemon":
		return runDaemon(args[1:])
	case "export":
		return runExport(args[1:])
	case "-h", "--help", "help":
		return usageError()
	default:
		return fmt.Errorf("unknown subcommand %q\n\n%s", args[0], usageText())
	}
}

func runDaemon(args []string) error {
	runtimePaths, err := paths.DefaultRuntimePaths()
	if err != nil {
		return err
	}

	defaultOutput, err := paths.Expand(runtimePaths.DataFile)
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	intervalSeconds := fs.Int("interval", 60, "sampling interval in seconds")
	fs.IntVar(intervalSeconds, "i", 60, "sampling interval in seconds")

	bufferSize := fs.Int("buffer-size", 20, "number of successful samples per append")
	fs.IntVar(bufferSize, "b", 20, "number of successful samples per append")

	outputPath := fs.String("output", defaultOutput, "binary log output path")
	fs.StringVar(outputPath, "o", defaultOutput, "binary log output path")

	compactAfter := fs.Int("compact-after", 100, "number of successful batch appends between compactions")
	fs.IntVar(compactAfter, "c", 100, "number of successful batch appends between compactions")

	sampleTimeout := fs.Int("sample-timeout", 40, "timeout for the location-cmd")
	fs.IntVar(sampleTimeout, "t", 40, "timeout for the location-cmd")

	locationCmd := fs.String("location-cmd", "termux-location -p passive -r last", "location command to execute")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments for daemon: %v", fs.Args())
	}

	if *intervalSeconds <= 0 {
		return errors.New("--interval must be greater than zero")
	}
	if *bufferSize <= 0 {
		return errors.New("--buffer-size must be greater than zero")
	}
	if *compactAfter <= 0 {
		return errors.New("--compact-after must be greater than zero")
	}

	if *sampleTimeout <= 0 {
		return errors.New("--sample-timeout must be greater than zero")
	}

	resolvedOutput, err := paths.Expand(*outputPath)
	if err != nil {
		return err
	}

	if err := paths.EnsureDir(runtimePaths.AppDir); err != nil {
		return err
	}

	cfg := daemon.Config{
		Interval:      time.Duration(*intervalSeconds) * time.Second,
		BufferSize:    *bufferSize,
		OutputPath:    resolvedOutput,
		CompactAfter:  *compactAfter,
		LocationCmd:   *locationCmd,
		SampleTimeout: time.Duration(*sampleTimeout) * time.Second,
		PendingCap:    *bufferSize * 10,
		LockPath:      runtimePaths.LockFile,
		PIDPath:       runtimePaths.PIDFile,
		LogPath:       runtimePaths.DaemonLog,
	}

	return daemon.Start(cfg)
}

func runExport(args []string) error {
	runtimePaths, err := paths.DefaultRuntimePaths()
	if err != nil {
		return err
	}

	defaultInput, err := paths.Expand(runtimePaths.DataFile)
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	inputPath := fs.String("input", defaultInput, "binary log input path")
	fs.StringVar(inputPath, "i", defaultInput, "binary log input path")

	outputPath := fs.String("output", "", "csv output path (stdout when omitted)")
	fs.StringVar(outputPath, "o", "", "csv output path (stdout when omitted)")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments for export: %v", fs.Args())
	}

	resolvedInput, err := paths.Expand(*inputPath)
	if err != nil {
		return err
	}
	if _, err := os.Stat(resolvedInput); err != nil {
		return fmt.Errorf("input log file %q is not readable: %w", resolvedInput, err)
	}

	resolvedOutput := ""
	if *outputPath != "" {
		resolvedOutput, err = paths.Expand(*outputPath)
		if err != nil {
			return err
		}
	}

	return exporter.Export(resolvedInput, resolvedOutput)
}

func usageError() error {
	return errors.New(usageText())
}

func usageText() string {
	return `Usage:
  location-logger daemon [--interval 60] [--buffer-size 20] [--output ~/.location_logger/data.bin] [--compact-after 100] [--sample-timeout 40] [--location-cmd "termux-location -p passive -r last"]
  location-logger export [--input ~/.location_logger/data.bin] [--output /path/to/output.csv]

Commands:
  daemon   Start detached background logger daemon
  export   Export binary log data to CSV (stdout when --output omitted)`
}
