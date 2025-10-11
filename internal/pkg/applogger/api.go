package applogger

import "log/slog"

// AppLogger defines the logging interface for the application.
type AppLogger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Warn(msg string, args ...any)
	Debug(msg string, args ...any)
	Log(level slog.Level, msg string, args ...any)
	Trace(msg string, args ...any)
}
