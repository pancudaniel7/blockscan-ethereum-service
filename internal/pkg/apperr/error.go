package apperr

// BaseError defines the interface for application-specific errors.
type BaseError interface {
	error
	Code() string
	Message() string
	Cause() error
}
