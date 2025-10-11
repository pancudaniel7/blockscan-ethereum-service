package applogger

import (
	"context"
	"log/slog"
	"os"
)

// AppDefaultLogger wraps slog.Logger and implements AppLogger.
type AppDefaultLogger struct {
	logger *slog.Logger
}

// NewAppDefaultLogger creates a new AppDefaultLogger with default options.
func NewAppDefaultLogger() *AppDefaultLogger {
	return &AppDefaultLogger{
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

func (l *AppDefaultLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

func (l *AppDefaultLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

func (l *AppDefaultLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

func (l *AppDefaultLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

func (l *AppDefaultLogger) Log(level slog.Level, msg string, args ...any) {
	l.logger.Log(context.Background(), level, msg, args...)
}

func (l *AppDefaultLogger) Trace(msg string, args ...any) {
	l.logger.Log(context.Background(), slog.Level(-8), msg, args...)
}
