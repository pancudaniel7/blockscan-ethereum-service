package apperr

import "fmt"

type InvalidArgErr struct {
	Msg   string
	Cause error
}

func (e *InvalidArgErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[INVALID_ARGUMENT] %s: %v", e.Msg, e.Cause)
	}
	return "[INVALID_ARGUMENT] " + e.Msg
}
func (e *InvalidArgErr) Code() string      { return "INVALID_ARGUMENT" }
func (e *InvalidArgErr) Message() string   { return e.Msg }
func (e *InvalidArgErr) CauseError() error { return e.Cause }
func (e *InvalidArgErr) Unwrap() error     { return e.Cause }

type NotFoundErr struct {
	Msg   string
	Cause error
}

func (e *NotFoundErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[NOT_FOUND] %s: %v", e.Msg, e.Cause)
	}
	return "[NOT_FOUND] " + e.Msg
}
func (e *NotFoundErr) Code() string      { return "NOT_FOUND" }
func (e *NotFoundErr) Message() string   { return e.Msg }
func (e *NotFoundErr) CauseError() error { return e.Cause }
func (e *NotFoundErr) Unwrap() error     { return e.Cause }

type AlreadyExistsErr struct {
	Msg   string
	Cause error
}

func (e *AlreadyExistsErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[ALREADY_EXISTS] %s: %v", e.Msg, e.Cause)
	}
	return "[ALREADY_EXISTS] " + e.Msg
}
func (e *AlreadyExistsErr) Code() string      { return "ALREADY_EXISTS" }
func (e *AlreadyExistsErr) Message() string   { return e.Msg }
func (e *AlreadyExistsErr) CauseError() error { return e.Cause }
func (e *AlreadyExistsErr) Unwrap() error     { return e.Cause }

type NotAuthorizedErr struct {
	Msg   string
	Cause error
}

func (e *NotAuthorizedErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[NOT_AUTHORIZED] %s: %v", e.Msg, e.Cause)
	}
	return "[NOT_AUTHORIZED] " + e.Msg
}
func (e *NotAuthorizedErr) Code() string      { return "NOT_AUTHORIZED" }
func (e *NotAuthorizedErr) Message() string   { return e.Msg }
func (e *NotAuthorizedErr) CauseError() error { return e.Cause }
func (e *NotAuthorizedErr) Unwrap() error     { return e.Cause }

type InternalErr struct {
	Msg   string
	Cause error
}

func (e *InternalErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[INTERNAL_ERROR] %s: %v", e.Msg, e.Cause)
	}
	return "[INTERNAL_ERROR] " + e.Msg
}
func (e *InternalErr) Code() string      { return "INTERNAL_ERROR" }
func (e *InternalErr) Message() string   { return e.Msg }
func (e *InternalErr) CauseError() error { return e.Cause }
func (e *InternalErr) Unwrap() error     { return e.Cause }

type BlockScanErr struct {
	Msg   string
	Cause error
}

func (e *BlockScanErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[BLOCKSCAN_ERROR] %s: %v", e.Msg, e.Cause)
	}
	return "[BLOCKSCAN_ERROR] " + e.Msg
}
func (e *BlockScanErr) Code() string      { return "BLOCKSCAN_ERROR" }
func (e *BlockScanErr) Message() string   { return e.Msg }
func (e *BlockScanErr) CauseError() error { return e.Cause }
func (e *BlockScanErr) Unwrap() error     { return e.Cause }

type BlockStoreErr struct {
	Msg   string
	Cause error
}

func (e *BlockStoreErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[BLOCKLOCK_ERROR] %s: %v", e.Msg, e.Cause)
	}
	return "[BLOCKLOCK_ERROR] " + e.Msg
}
func (e *BlockStoreErr) Code() string      { return "BLOCKLOCK_ERROR" }
func (e *BlockStoreErr) Message() string   { return e.Msg }
func (e *BlockStoreErr) CauseError() error { return e.Cause }
func (e *BlockStoreErr) Unwrap() error     { return e.Cause }

type BlockStreamErr struct {
	Msg   string
	Cause error
}

func (e *BlockStreamErr) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[BLOCKSTREAM_ERROR] %s: %v", e.Msg, e.Cause)
	}
	return "[BLOCKSTREAM_ERROR] " + e.Msg
}
func (e *BlockStreamErr) Code() string      { return "BLOCKSTREAM_ERROR" }
func (e *BlockStreamErr) Message() string   { return e.Msg }
func (e *BlockStreamErr) CauseError() error { return e.Cause }
func (e *BlockStreamErr) Unwrap() error     { return e.Cause }
