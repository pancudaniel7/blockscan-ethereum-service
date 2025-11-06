package apperr

import "fmt"

const (
	invalidArgumentCode = "INVALID_ARGUMENT"
	notFoundCode        = "NOT_FOUND"
	alreadyExistsCode   = "ALREADY_EXISTS"
	notAuthorizedCode   = "NOT_AUTHORIZED"
	internalErrorCode   = "INTERNAL_ERROR"
	blockScanCode       = "BLOCKSCAN_ERROR"
	blockStoreCode      = "BLOCKSTORE_ERROR"
	blockStreamCode     = "BLOCKSTREAM_ERROR"
)

type messageCause struct {
	Msg   string
	Cause error
}

func (e *messageCause) Message() string   { return e.Msg }
func (e *messageCause) CauseError() error { return e.Cause }
func (e *messageCause) Unwrap() error     { return e.Cause }

func formatError(code, msg string, cause error) string {
	if cause != nil {
		return fmt.Sprintf("[%s] %s: %v", code, msg, cause)
	}
	return fmt.Sprintf("[%s] %s", code, msg)
}

type InvalidArgErr struct {
	messageCause
}

func NewInvalidArgErr(msg string, cause error) *InvalidArgErr {
	return &InvalidArgErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *InvalidArgErr) Error() string { return formatError(invalidArgumentCode, e.Msg, e.Cause) }
func (e *InvalidArgErr) Code() string  { return invalidArgumentCode }

type NotFoundErr struct {
	messageCause
}

func NewNotFoundErr(msg string, cause error) *NotFoundErr {
	return &NotFoundErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *NotFoundErr) Error() string { return formatError(notFoundCode, e.Msg, e.Cause) }
func (e *NotFoundErr) Code() string  { return notFoundCode }

type AlreadyExistsErr struct {
	messageCause
}

func NewAlreadyExistsErr(msg string, cause error) *AlreadyExistsErr {
	return &AlreadyExistsErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *AlreadyExistsErr) Error() string { return formatError(alreadyExistsCode, e.Msg, e.Cause) }
func (e *AlreadyExistsErr) Code() string  { return alreadyExistsCode }

type NotAuthorizedErr struct {
	messageCause
}

func NewNotAuthorizedErr(msg string, cause error) *NotAuthorizedErr {
	return &NotAuthorizedErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *NotAuthorizedErr) Error() string { return formatError(notAuthorizedCode, e.Msg, e.Cause) }
func (e *NotAuthorizedErr) Code() string  { return notAuthorizedCode }

type InternalErr struct {
	messageCause
}

func NewInternalErr(msg string, cause error) *InternalErr {
	return &InternalErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *InternalErr) Error() string { return formatError(internalErrorCode, e.Msg, e.Cause) }
func (e *InternalErr) Code() string  { return internalErrorCode }

type BlockScanErr struct {
	messageCause
}

func NewBlockScanErr(msg string, cause error) *BlockScanErr {
	return &BlockScanErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *BlockScanErr) Error() string { return formatError(blockScanCode, e.Msg, e.Cause) }
func (e *BlockScanErr) Code() string  { return blockScanCode }

type BlockStoreErr struct {
	messageCause
}

func NewBlockStoreErr(msg string, cause error) *BlockStoreErr {
	return &BlockStoreErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *BlockStoreErr) Error() string { return formatError(blockStoreCode, e.Msg, e.Cause) }
func (e *BlockStoreErr) Code() string  { return blockStoreCode }

type BlockStreamErr struct {
	messageCause
}

func NewBlockStreamErr(msg string, cause error) *BlockStreamErr {
	return &BlockStreamErr{messageCause: messageCause{Msg: msg, Cause: cause}}
}

func (e *BlockStreamErr) Error() string { return formatError(blockStreamCode, e.Msg, e.Cause) }
func (e *BlockStreamErr) Code() string  { return blockStreamCode }
