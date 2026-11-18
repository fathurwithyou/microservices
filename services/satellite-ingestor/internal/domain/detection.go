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
	// FirstOffset is the feed offset at which this key was first seen. It is
	// state, not identity: it never takes part in Key.
	FirstOffset int64
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

// NormalizeConfidence maps the spellings a feed may use onto l/n/h. FIRMS
// VIIRS sends "l", "n", "h"; the long names and the MODIS percentage scale
// are accepted so a feed that changes spelling does not turn every detection
// into an unknown class. An unknown value is kept as is and ranks lowest
// without being treated as low confidence.
func NormalizeConfidence(raw string) Confidence {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "l", "low":
		return ConfidenceLow
	case "n", "nominal", "normal", "medium":
		return ConfidenceNominal
	case "h", "high":
		return ConfidenceHigh
	}
	if percent, err := strconv.ParseFloat(value, 64); err == nil {
		switch {
		case percent < 30:
			return ConfidenceLow
		case percent >= 80:
			return ConfidenceHigh
		default:
			return ConfidenceNominal
		}
	}
	return Confidence(value)
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
	hour, minute, ok := clockOf(d.AcqTime)
	if !ok {
		return time.Time{}, fmt.Errorf("%w: acq_time %q", ErrInvalidDetectionTime, d.AcqTime)
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, time.UTC), nil
}

// clockOf reads an HHMM acquisition time. FIRMS writes it as an integer, so
// 02:43 arrives as "243" and 00:05 as "5"; "02:43" and "0243" are accepted
// too. The source text itself is never rewritten because it is part of the
// detection key.
func clockOf(raw string) (hour, minute int, ok bool) {
	value := strings.TrimSpace(raw)
	if before, after, found := strings.Cut(value, ":"); found {
		h, errH := strconv.Atoi(before)
		m, errM := strconv.Atoi(after)
		if errH != nil || errM != nil {
			return 0, 0, false
		}
		hour, minute = h, m
	} else {
		if value == "" || len(value) > 4 {
			return 0, 0, false
		}
		number, err := strconv.Atoi(value)
		if err != nil || number < 0 {
			return 0, 0, false
		}
		hour, minute = number/100, number%100
	}
	if hour > 23 || minute > 59 || hour < 0 || minute < 0 {
		return 0, 0, false
	}
	return hour, minute, true
}
