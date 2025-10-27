package applog

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// DefaultLogger wraps slog.logger and implements AppLogger.
type DefaultLogger struct {
	logger *slog.Logger
}

// NewAppDefaultLogger creates a new DefaultLogger with default options.
func NewAppDefaultLogger() *DefaultLogger {
	levelStr := viper.GetString("log.level")
	level := parseLogLevel(levelStr)
	return &DefaultLogger{
		logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})),
	}
}

func (l *DefaultLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

func (l *DefaultLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

func (l *DefaultLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

func (l *DefaultLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

func (l *DefaultLogger) Trace(msg string, args ...any) {
	l.logger.Log(context.Background(), slog.Level(-8), msg, args...)
}

func (l *DefaultLogger) Fatal(msg string, args ...any) {
	l.logger.Error(msg, args...)
	os.Exit(1)
}

func parseLogLevel(s string) slog.Level {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "trace":
		return slog.Level(-8)
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
