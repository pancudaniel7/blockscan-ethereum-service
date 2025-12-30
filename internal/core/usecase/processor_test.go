package usecase

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type stubLogger struct{}

func (stubLogger) Info(string, ...any)  {}
func (stubLogger) Warn(string, ...any)  {}
func (stubLogger) Error(string, ...any) {}
func (stubLogger) Debug(string, ...any) {}
func (stubLogger) Trace(string, ...any) {}
func (stubLogger) Fatal(string, ...any) {}

type fakeStoreLogger struct {
	storeRes  bool
	storeErr  error
	pubExists bool
	pubErr    error
	setPubRes bool
	setPubErr error
}

func (f *fakeStoreLogger) StoreBlock(ctx context.Context, block *entity.Block) (bool, error) {
	return f.storeRes, f.storeErr
}
func (f *fakeStoreLogger) StorePublishedBlockHash(ctx context.Context, blockHash string) (bool, error) {
	return f.setPubRes, f.setPubErr
}
func (f *fakeStoreLogger) IsBlockPublished(ctx context.Context, blockHash string) (bool, error) {
	return f.pubExists, f.pubErr
}

type fakePublisher struct {
	err         error
	lastHeaders map[string]string
}

func (f *fakePublisher) PublishBlock(ctx context.Context, block *entity.Block, headers map[string]string) error {
	f.lastHeaders = headers
	return f.err
}

type nopStreamReader struct{}

func (nopStreamReader) SetHandler(port.StreamMessageHandler) {}
func (nopStreamReader) StartReadFromStream() error           { return nil }
func (nopStreamReader) StopReadFromStream()                  {}

func TestBlockProcessorService_StoreBlock(t *testing.T) {
	cases := []struct {
		name    string
		store   *fakeStoreLogger
		blk     *types.Block
		wantErr bool
	}{
		{name: "nil block", store: &fakeStoreLogger{}, blk: nil, wantErr: true},
		{name: "store error", store: &fakeStoreLogger{storeErr: errors.New("db")}, blk: types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)}), wantErr: true},
		{name: "stored true", store: &fakeStoreLogger{storeRes: true}, blk: types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)})},
		{name: "stored false", store: &fakeStoreLogger{storeRes: false}, blk: types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)})},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc := NewBlockProcessorService(stubLogger{}, tc.store, nopStreamReader{}, &fakePublisher{})
			err := svc.StoreBlock(context.Background(), tc.blk)
			if tc.wantErr {
				var be *apperr.BlockProcessErr
				require.ErrorAs(t, err, &be)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBlockProcessorService_ReadAndPublishBlock(t *testing.T) {
	payloadOK, _ := MarshalBlockJSON(&entity.Block{Header: entity.Header{Number: 0}})
	msgOK := redis.XMessage{ID: "42-0", Values: map[string]interface{}{"hash": "0xabc", "number": "7", "payload": string(payloadOK)}}
	cases := []struct {
		name       string
		ctx        context.Context
		store      *fakeStoreLogger
		pub        *fakePublisher
		msg        redis.XMessage
		wantErrIs  error
		wantErrAs  bool
		wantHeader string
	}{
		{name: "context canceled", ctx: canceledCtx(), store: &fakeStoreLogger{}, pub: &fakePublisher{}, msg: redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash": "0x1", "number": "1", "payload": "{}"}}, wantErrIs: context.Canceled},
		{name: "missing hash", ctx: context.Background(), store: &fakeStoreLogger{}, pub: &fakePublisher{}, msg: redis.XMessage{ID: "0-1", Values: map[string]interface{}{"number": "1", "payload": "{}"}}, wantErrAs: true},
		{name: "invalid number", ctx: context.Background(), store: &fakeStoreLogger{}, pub: &fakePublisher{}, msg: redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash": "0x1", "number": "x", "payload": "{}"}}, wantErrAs: true},
		{name: "missing payload", ctx: context.Background(), store: &fakeStoreLogger{}, pub: &fakePublisher{}, msg: redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash": "0x1", "number": "1"}}, wantErrAs: true},
		{name: "bad payload", ctx: context.Background(), store: &fakeStoreLogger{}, pub: &fakePublisher{}, msg: redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash": "0x1", "number": "1", "payload": "{"}}, wantErrAs: true},
		{name: "marker exists", ctx: context.Background(), store: &fakeStoreLogger{pubExists: true}, pub: &fakePublisher{}, msg: msgOK},
		{name: "publish error", ctx: context.Background(), store: &fakeStoreLogger{}, pub: &fakePublisher{err: errors.New("kafka")}, msg: msgOK, wantErrAs: true},
		{name: "store marker error", ctx: context.Background(), store: &fakeStoreLogger{setPubErr: errors.New("redis")}, pub: &fakePublisher{}, msg: msgOK, wantErrAs: true},
		{name: "success", ctx: context.Background(), store: &fakeStoreLogger{setPubRes: true}, pub: &fakePublisher{}, msg: msgOK, wantHeader: "42-0"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc := NewBlockProcessorService(stubLogger{}, tc.store, nopStreamReader{}, tc.pub)
			err := svc.ReadAndPublishBlock(tc.ctx, tc.msg)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
				return
			}
			if tc.wantErrAs {
				var be *apperr.BlockProcessErr
				require.ErrorAs(t, err, &be)
				return
			}
			require.NoError(t, err)
			if tc.wantHeader != "" {
				require.Equal(t, tc.wantHeader, tc.pub.lastHeaders["source-message-id"])
			}
		})
	}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
