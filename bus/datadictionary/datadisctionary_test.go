package datadictionary

import (
    "testing"
    "time"
    "bytes"
    "os"

    "github.com/edancain/RocketLab/types"
    "github.com/edancain/RocketLab/bus/logger"
)

type levelWriter struct {
    bytes.Buffer
    level logger.LogLevel
}

func (lw *levelWriter) Write(p []byte) (n int, err error) {
    if logger.GetLogLevel() >= lw.level {
        return lw.Buffer.Write(p)
    }
    return len(p), nil
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

            errorWriter := &levelWriter{level: logger.LevelError}
            infoWriter := &levelWriter{level: logger.LevelInfo}
            debugWriter := &levelWriter{level: logger.LevelDebug}

            logger.ErrorLogger.SetOutput(errorWriter)
            logger.InfoLogger.SetOutput(infoWriter)
            logger.DebugLogger.SetOutput(debugWriter)

            dd := NewDataDictionary()
            
            // Trigger all types of logs
            msg := types.Message{Timestamp: time.Now(), Topic: "test-topic", Content: "Test message"}
            for i := 0; i < maxMessagesPerTopic+1; i++ {
                err := dd.Store(msg)
                if err != nil && i < maxMessagesPerTopic {
                    t.Errorf("Unexpected error storing message: %v", err)
                }
            }

            gotError := errorWriter.Len() > 0
            gotInfo := infoWriter.Len() > 0
            gotDebug := debugWriter.Len() > 0

            if gotError != tt.wantError {
                t.Errorf("Error logging: got %v, want %v", gotError, tt.wantError)
            }

            if gotInfo != tt.wantInfo {
                t.Errorf("Info logging: got %v, want %v", gotInfo, tt.wantInfo)
            }

            if gotDebug != tt.wantDebug {
                t.Errorf("Debug logging: got %v, want %v", gotDebug, tt.wantDebug)
            }

            // Reset log outputs
            logger.ErrorLogger.SetOutput(os.Stderr)
            logger.InfoLogger.SetOutput(os.Stdout)
            logger.DebugLogger.SetOutput(os.Stdout)
        })
    }
}

func TestStore(t *testing.T) {
    logger.SetLogLevel(logger.LevelDebug)
    var logBuffer bytes.Buffer
    logger.ErrorLogger.SetOutput(&logBuffer)
    logger.InfoLogger.SetOutput(&logBuffer)
    logger.DebugLogger.SetOutput(&logBuffer)

    dd := NewDataDictionary()
    msg := types.Message{
        Timestamp: time.Now(),
        Topic:     "test-topic",
        Content:   "Hello, World!",
    }

    err := dd.Store(msg)
    if err != nil {
        t.Errorf("Store failed: %v", err)
    }

    if !bytes.Contains(logBuffer.Bytes(), []byte("New topic created in DataDictionary: test-topic")) {
        t.Error("Expected log message for new topic creation")
    }

    // Test storing more than maxMessagesPerTopic
    logBuffer.Reset()
    for i := 0; i < maxMessagesPerTopic; i++ {
        err = dd.Store(msg)
        if err != nil && i < maxMessagesPerTopic-1 {
            t.Errorf("Store failed unexpectedly: %v", err)
        }
    }

    if err == nil {
        t.Error("Expected error when exceeding maxMessagesPerTopic, got nil")
    }

    if !bytes.Contains(logBuffer.Bytes(), []byte("max messages per topic reached for topic test-topic")) {
        t.Error("Expected log message for max messages reached")
    }
}

func TestCleanupExpiredMessages(t *testing.T) {
    logger.SetLogLevel(logger.LevelDebug)
    var logBuffer bytes.Buffer
    logger.ErrorLogger.SetOutput(&logBuffer)
    logger.InfoLogger.SetOutput(&logBuffer)
    logger.DebugLogger.SetOutput(&logBuffer)

    dd := NewDataDictionary()
    now := time.Now()

    msgs := []types.Message{
        {Timestamp: now.Add(-25 * time.Hour), Topic: "test-topic", Content: "Expired message"},
        {Timestamp: now.Add(-23 * time.Hour), Topic: "test-topic", Content: "Almost expired message"},
        {Timestamp: now, Topic: "test-topic", Content: "Current message"},
    }

    for _, msg := range msgs {
        err := dd.Store(msg)
        if err != nil {
            t.Errorf("Store failed: %v", err)
        }
    }

    dd.cleanupExpiredMessages()

    retrieved := dd.GetMessages("test-topic", now.Add(-26*time.Hour), now.Add(time.Hour))
    if len(retrieved) != 2 {
        t.Errorf("Expected 2 messages after cleanup, got %d", len(retrieved))
    }

    for _, msg := range retrieved {
        if msg.Content == "Expired message" {
            t.Error("Found expired message that should have been cleaned up")
        }
    }

    if !bytes.Contains(logBuffer.Bytes(), []byte("Cleaned up 1 messages for topic test-topic")) {
        t.Error("Expected log message for cleanup")
    }
}
