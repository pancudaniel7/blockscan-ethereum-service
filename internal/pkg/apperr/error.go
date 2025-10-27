package apperr

// BaseError defines the interface for application-specific errors.
type BaseError interface {
    error
    Code() string
    Message() string
    // CauseError returns the underlying cause, if any.
    CauseError() error
}
