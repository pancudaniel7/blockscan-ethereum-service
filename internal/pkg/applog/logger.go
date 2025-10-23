package applog

type AppLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	Trace(msg string, args ...any)
	Fatal(msg string, args ...any)
}
