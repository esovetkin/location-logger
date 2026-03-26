package storage

import (
	"math"
	"time"
)

type Record struct {
	TimestampUTC time.Time

	Latitude         float64
	Longitude        float64
	Altitude         float32
	Accuracy         float32
	VerticalAccuracy float32
	Bearing          float32
	Speed            float32

	ElapsedMs *uint32
	Provider  *string
}

func NewRecordWithMissing(ts time.Time) Record {
	return Record{
		TimestampUTC:     ts.UTC(),
		Latitude:         math.NaN(),
		Longitude:        math.NaN(),
		Altitude:         float32(math.NaN()),
		Accuracy:         float32(math.NaN()),
		VerticalAccuracy: float32(math.NaN()),
		Bearing:          float32(math.NaN()),
		Speed:            float32(math.NaN()),
		ElapsedMs:        nil,
		Provider:         nil,
	}
}
