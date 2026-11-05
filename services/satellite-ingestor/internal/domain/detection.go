package domain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Satellite identifies the source platform of a hotspot detection.
type Satellite string

// Confidence is the FIRMS confidence class of a detection.
type Confidence string

const (
	ConfidenceLow     Confidence = "l"
	ConfidenceNominal Confidence = "n"
	ConfidenceHigh    Confidence = "h"
)

var ErrInvalidDetectionTime = errors.New("invalid detection time")

// Detection is one raw satellite hotspot observation. AcqDate and AcqTime keep
// the source strings so the detection key stays stable across feeds.
type Detection struct {
	Satellite Satellite
	AcqDate   string
	AcqTime   string
	Latitude  float64
	Longitude float64
	// RawLatitude and RawLongitude keep the lexical form from the feed so
	// detection keys match the audit contract without rounding.
	RawLatitude  string
	RawLongitude string
	Confidence   Confidence
	FRP          float64
}

// Key is the stable identity of a detection, built from unrounded source values.
func (d Detection) Key() string {
	latitude := d.RawLatitude
	if latitude == "" {
		latitude = strconv.FormatFloat(d.Latitude, 'f', -1, 64)
	}
	longitude := d.RawLongitude
	if longitude == "" {
		longitude = strconv.FormatFloat(d.Longitude, 'f', -1, 64)
	}
	return strings.Join([]string{
		string(d.Satellite),
		d.AcqDate,
		d.AcqTime,
		latitude,
		longitude,
	}, "|")
}

// Ignored reports whether the detection must be dropped before clustering.
func (d Detection) Ignored() bool {
	return d.Confidence == ConfidenceLow
}

// Rank orders confidence classes from least to most severe.
func (c Confidence) Rank() int {
	switch c {
	case ConfidenceHigh:
		return 2
	case ConfidenceNominal:
		return 1
	default:
		return 0
	}
}

// Time combines the acquisition date and hhmm time into a UTC instant.
func (d Detection) Time() (time.Time, error) {
	date, err := time.Parse("2006-01-02", d.AcqDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: acq_date %q", ErrInvalidDetectionTime, d.AcqDate)
	}
	clock, err := time.Parse("1504", d.AcqTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: acq_time %q", ErrInvalidDetectionTime, d.AcqTime)
	}
	return time.Date(date.Year(), date.Month(), date.Day(), clock.Hour(), clock.Minute(), 0, 0, time.UTC), nil
}
