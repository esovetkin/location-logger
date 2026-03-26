package daemon

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestSampleOnceFullPayload(t *testing.T) {
	cmd := `printf '%s' '{"latitude":50.86697859,"longitude":6.12225608,"altitude":184.26806640625,"accuracy":10.052664756774902,"vertical_accuracy":4.0,"bearing":0.0,"speed":0.0,"elapsedMs":6,"provider":"gps"}'`

	rec, err := sampleOnce(context.Background(), cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("sampleOnce returned error: %v", err)
	}

	if rec.Latitude != 50.86697859 || rec.Longitude != 6.12225608 {
		t.Fatalf("coordinates mismatch: got (%v,%v)", rec.Latitude, rec.Longitude)
	}
	if math.IsNaN(float64(rec.Altitude)) {
		t.Fatal("altitude should be present")
	}
	if rec.ElapsedMs == nil || *rec.ElapsedMs != 6 {
		t.Fatalf("elapsedMs mismatch: %v", rec.ElapsedMs)
	}
	if rec.Provider == nil || *rec.Provider != "gps" {
		t.Fatalf("provider mismatch: %v", rec.Provider)
	}
}

func TestSampleOnceMissingFieldsMapToNaNAndNil(t *testing.T) {
	cmd := `printf '%s' '{"latitude":50.0,"longitude":6.0}'`

	rec, err := sampleOnce(context.Background(), cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("sampleOnce returned error: %v", err)
	}

	if rec.Latitude != 50.0 || rec.Longitude != 6.0 {
		t.Fatalf("coordinates mismatch: got (%v,%v)", rec.Latitude, rec.Longitude)
	}
	if !math.IsNaN(float64(rec.Altitude)) {
		t.Fatal("altitude should be NaN when missing")
	}
	if !math.IsNaN(float64(rec.Accuracy)) {
		t.Fatal("accuracy should be NaN when missing")
	}
	if rec.ElapsedMs != nil {
		t.Fatal("elapsedMs should be nil when missing")
	}
	if rec.Provider != nil {
		t.Fatal("provider should be nil when missing")
	}
}

func TestSampleOnceEmptyOutput(t *testing.T) {
	_, err := sampleOnce(context.Background(), `printf ''`, 2*time.Second)
	if err == nil {
		t.Fatal("expected error for empty output")
	}
	if !strings.Contains(err.Error(), "empty location output") {
		t.Fatalf("expected empty output error, got: %v", err)
	}
}

func TestSampleOnceMalformedJSON(t *testing.T) {
	_, err := sampleOnce(context.Background(), `printf '%s' '{'`, 2*time.Second)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
	if !strings.Contains(err.Error(), "parse location JSON") {
		t.Fatalf("expected parse error, got: %v", err)
	}
}
