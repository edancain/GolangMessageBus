package backpressure

import (
	"testing"
	"time"
)

func TestCheckPressure(t *testing.T) {
	bpm := NewBackPressureManager()

	// Test normal operation
	for i := 0; i < 1000; i++ {
		err := bpm.CheckPressure("test-topic")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Test back pressure
	err := bpm.CheckPressure("test-topic")
	if err == nil {
		t.Error("Expected back pressure error, got nil")
	}

	// Test recovery after 1 second
	time.Sleep(1 * time.Second)
	err = bpm.CheckPressure("test-topic")
	if err != nil {
		t.Errorf("Unexpected error after recovery period: %v", err)
	}
}

func TestMultipleTopics(t *testing.T) {
	bpm := NewBackPressureManager()

	// Test two topics simultaneously
	for i := 0; i < 1000; i++ {
		err := bpm.CheckPressure("topic1")
		if err != nil {
			t.Errorf("Unexpected error for topic1: %v", err)
		}

		err = bpm.CheckPressure("topic2")
		if err != nil {
			t.Errorf("Unexpected error for topic2: %v", err)
		}
	}

	// Both topics should hit back pressure
	err := bpm.CheckPressure("topic1")
	if err == nil {
		t.Error("Expected back pressure error for topic1, got nil")
	}

	err = bpm.CheckPressure("topic2")
	if err == nil {
		t.Error("Expected back pressure error for topic2, got nil")
	}
}
