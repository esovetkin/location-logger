package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"location_logger/internal/storage"
)

type termuxLocation struct {
	Latitude         *float64 `json:"latitude"`
	Longitude        *float64 `json:"longitude"`
	Altitude         *float32 `json:"altitude"`
	Accuracy         *float32 `json:"accuracy"`
	VerticalAccuracy *float32 `json:"vertical_accuracy"`
	Bearing          *float32 `json:"bearing"`
	Speed            *float32 `json:"speed"`
	ElapsedMs        *uint32  `json:"elapsedMs"`
	Provider         *string  `json:"provider"`
}

func sampleOnce(ctx context.Context, cmdStr string, timeout time.Duration) (storage.Record, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	out, err := runLocationCmd(queryCtx, cmdStr)
	if err != nil {
		return storage.Record{}, err
	}

	var payload termuxLocation
	if err := json.Unmarshal(out, &payload); err != nil {
		return storage.Record{}, fmt.Errorf("parse location JSON: %w", err)
	}

	rec := normalize(payload, time.Now().UTC())
	return rec, nil
}

func runLocationCmd(ctx context.Context, cmdStr string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "sh", "-lc", cmdStr)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("run location command %q: %w", cmdStr, err)
	}

	out = bytes.TrimSpace(out)
	if len(out) == 0 {
		return nil, errors.New("empty location output")
	}

	return out, nil
}

func normalize(input termuxLocation, ts time.Time) storage.Record {
	rec := storage.NewRecordWithMissing(ts)

	if input.Latitude != nil {
		rec.Latitude = *input.Latitude
	}
	if input.Longitude != nil {
		rec.Longitude = *input.Longitude
	}
	if input.Altitude != nil {
		rec.Altitude = *input.Altitude
	}
	if input.Accuracy != nil {
		rec.Accuracy = *input.Accuracy
	}
	if input.VerticalAccuracy != nil {
		rec.VerticalAccuracy = *input.VerticalAccuracy
	}
	if input.Bearing != nil {
		rec.Bearing = *input.Bearing
	}
	if input.Speed != nil {
		rec.Speed = *input.Speed
	}
	rec.ElapsedMs = input.ElapsedMs
	rec.Provider = input.Provider

	return rec
}
