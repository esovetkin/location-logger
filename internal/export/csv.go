package export

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"location_logger/internal/paths"
	"location_logger/internal/storage"
)

func Export(inputPath, outputPath string) error {
	return exportWithWriter(inputPath, outputPath, os.Stdout)
}

func exportWithWriter(inputPath, outputPath string, stdout io.Writer) error {
	records, err := storage.ReadAll(inputPath)
	if err != nil {
		return fmt.Errorf("read input log %q: %w", inputPath, err)
	}

	writer, closeFn, err := csvWriter(outputPath, stdout)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = closeFn()
		}
	}()

	w := csv.NewWriter(writer)
	if err := w.Write([]string{
		"timestamp_utc", "latitude", "longitude", "altitude", "accuracy",
		"vertical_accuracy", "bearing", "speed", "elapsedMs", "provider",
	}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}

	for i := range records {
		rec := records[i]
		if err := w.Write([]string{
			rec.TimestampUTC.UTC().Format(time.RFC3339Nano),
			f64(rec.Latitude),
			f64(rec.Longitude),
			f32(rec.Altitude),
			f32(rec.Accuracy),
			f32(rec.VerticalAccuracy),
			f32(rec.Bearing),
			f32(rec.Speed),
			u32(rec.ElapsedMs),
			str(rec.Provider),
		}); err != nil {
			return fmt.Errorf("write csv row %d: %w", i, err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("flush csv writer: %w", err)
	}

	if err := closeFn(); err != nil {
		return fmt.Errorf("close csv output: %w", err)
	}
	closed = true
	return nil
}

func csvWriter(outputPath string, stdout io.Writer) (io.Writer, func() error, error) {
	if stdout == nil {
		stdout = os.Stdout
	}

	if strings.TrimSpace(outputPath) == "" {
		return stdout, func() error { return nil }, nil
	}

	if err := paths.EnsureParentDir(outputPath); err != nil {
		return nil, nil, err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, fmt.Errorf("create output file %q: %w", outputPath, err)
	}
	return file, file.Close, nil
}

func f64(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func f32(value float32) string {
	if math.IsNaN(float64(value)) {
		return "NaN"
	}
	return strconv.FormatFloat(float64(value), 'f', -1, 32)
}

func u32(value *uint32) string {
	if value == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*value), 10)
}

func str(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
