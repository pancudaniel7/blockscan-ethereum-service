package applogger

import (
	"context"
	"log/slog"
	"os"
)

// DefaultLogger wraps slog.Logger and implements Logger.
type DefaultLogger struct {
	logger *slog.Logger
}

// NewAppDefaultLogger creates a new DefaultLogger with default options.
func NewAppDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

func (l *DefaultLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

func (l *DefaultLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

func (l *DefaultLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

func (l *DefaultLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

func (l *DefaultLogger) Log(level slog.Level, msg string, args ...any) {
	l.logger.Log(context.Background(), level, msg, args...)
}

func (l *DefaultLogger) Trace(msg string, args ...any) {
	l.logger.Log(context.Background(), slog.Level(-8), msg, args...)
}
