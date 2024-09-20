package backpressure

import (
	"testing"
	"time"
	"bytes"

	"github.com/edancain/RocketLab/bus/logger"
)

func TestCheckPressure(t *testing.T) {
	// Capture log output
	var logBuffer bytes.Buffer
	logger.ErrorLogger.SetOutput(&logBuffer)
	logger.InfoLogger.SetOutput(&logBuffer)
	logger.DebugLogger.SetOutput(&logBuffer)

	threshold := 1000
	bpm := NewBackPressureManager(threshold)

	// Test normal operation
	for i := 0; i < threshold; i++ {
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

	// Check if error was logged
	if !bytes.Contains(logBuffer.Bytes(), []byte("Back pressure applied")) {
		t.Error("Expected back pressure error to be logged")
	}

	// Test recovery after 1 second
	logBuffer.Reset()
	time.Sleep(1 * time.Second)
	err = bpm.CheckPressure("test-topic")
	if err != nil {
		t.Errorf("Unexpected error after recovery period: %v", err)
	}
}

func TestMultipleTopics(t *testing.T) {
	// Capture log output
	var logBuffer bytes.Buffer
	logger.ErrorLogger.SetOutput(&logBuffer)
	logger.InfoLogger.SetOutput(&logBuffer)
	logger.DebugLogger.SetOutput(&logBuffer)

	threshold := 1000
	bpm := NewBackPressureManager(threshold)

	// Test two topics simultaneously
	for i := 0; i < threshold; i++ {
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

	// Check if errors were logged
	if !bytes.Contains(logBuffer.Bytes(), []byte("Back pressure applied")) {
		t.Error("Expected back pressure errors to be logged")
	}
}

func TestLogLevels(t *testing.T) {
	tests := []struct {
		name      string
		logLevel  logger.LogLevel
		wantError bool
		wantInfo  bool
		wantDebug bool
	}{
		{"ErrorLevel", logger.LevelError, true, false, false},
		{"InfoLevel", logger.LevelInfo, true, true, false},
		{"DebugLevel", logger.LevelDebug, true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.SetLogLevel(tt.logLevel)

			var errorBuffer, infoBuffer, debugBuffer bytes.Buffer
			logger.ErrorLogger.SetOutput(&errorBuffer)
			logger.InfoLogger.SetOutput(&infoBuffer)
			logger.DebugLogger.SetOutput(&debugBuffer)

			bpm := NewBackPressureManager(10)
			
			// Trigger all types of logs
			for i := 0; i < 11; i++ {
				bpm.CheckPressure("test-topic")
			}

			gotError := errorBuffer.Len() > 0
			if gotError != tt.wantError {
				t.Errorf("Error logging: got %v, want %v", gotError, tt.wantError)
			}

			gotInfo := infoBuffer.Len() > 0
			if gotInfo != tt.wantInfo {
				t.Errorf("Info logging: got %v, want %v", gotInfo, tt.wantInfo)
			}

			gotDebug := debugBuffer.Len() > 0
			if gotDebug != tt.wantDebug {
				t.Errorf("Debug logging: got %v, want %v", gotDebug, tt.wantDebug)
			}
		})
	}
}