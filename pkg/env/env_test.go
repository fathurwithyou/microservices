package env

import (
	"testing"
	"time"
)

func TestString(t *testing.T) {
	t.Setenv("TEST_STRING", "value")
	if got := String("TEST_STRING", "fallback"); got != "value" {
		t.Fatalf("String = %q, want value", got)
	}
	if got := String("TEST_STRING_UNSET", "fallback"); got != "fallback" {
		t.Fatalf("String unset = %q, want fallback", got)
	}
}

func TestRequired(t *testing.T) {
	t.Setenv("TEST_REQUIRED", "value")
	if _, err := Required("TEST_REQUIRED"); err != nil {
		t.Fatalf("Required returned error: %v", err)
	}
	if _, err := Required("TEST_REQUIRED_UNSET"); err == nil {
		t.Fatal("Required on unset variable must fail")
	}
}

func TestIntRejectsMalformed(t *testing.T) {
	t.Setenv("TEST_INT", "abc")
	if _, err := Int("TEST_INT", 5); err == nil {
		t.Fatal("Int on malformed value must fail, not fall back")
	}
	t.Setenv("TEST_INT", "42")
	got, err := Int("TEST_INT", 5)
	if err != nil || got != 42 {
		t.Fatalf("Int = %d, %v; want 42, nil", got, err)
	}
	got, err = Int("TEST_INT_UNSET", 5)
	if err != nil || got != 5 {
		t.Fatalf("Int unset = %d, %v; want 5, nil", got, err)
	}
}

func TestDurationRejectsMalformed(t *testing.T) {
	t.Setenv("TEST_DURATION", "notaduration")
	if _, err := Duration("TEST_DURATION", time.Second); err == nil {
		t.Fatal("Duration on malformed value must fail, not fall back")
	}
	t.Setenv("TEST_DURATION", "2m")
	got, err := Duration("TEST_DURATION", time.Second)
	if err != nil || got != 2*time.Minute {
		t.Fatalf("Duration = %v, %v; want 2m, nil", got, err)
	}
}

func TestBoolRejectsMalformed(t *testing.T) {
	t.Setenv("TEST_BOOL", "maybe")
	if _, err := Bool("TEST_BOOL", false); err == nil {
		t.Fatal("Bool on malformed value must fail, not fall back")
	}
	t.Setenv("TEST_BOOL", "true")
	got, err := Bool("TEST_BOOL", false)
	if err != nil || !got {
		t.Fatalf("Bool = %v, %v; want true, nil", got, err)
	}
}
