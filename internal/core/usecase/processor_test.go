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

type fakeStoreLogger struct{
    storeRes bool
    storeErr error
    pubExists bool
    pubErr error
    setPubRes bool
    setPubErr error
}

func (f *fakeStoreLogger) StoreBlock(ctx context.Context, block *entity.Block) (bool, error) { return f.storeRes, f.storeErr }
func (f *fakeStoreLogger) StorePublishedBlockHash(ctx context.Context, blockHash string) (bool, error) { return f.setPubRes, f.setPubErr }
func (f *fakeStoreLogger) IsBlockPublished(ctx context.Context, blockHash string) (bool, error) { return f.pubExists, f.pubErr }

type fakePublisher struct{
    err error
    lastHeaders map[string]string
}
func (f *fakePublisher) PublishBlock(ctx context.Context, block *entity.Block, headers map[string]string) error {
    f.lastHeaders = headers
    return f.err
}

type nopStreamReader struct{ }
func (nopStreamReader) SetHandler(port.StreamMessageHandler) {}
func (nopStreamReader) StartReadFromStream() error { return nil }
func (nopStreamReader) StopReadFromStream() {}

// no helper needed; tests use types.NewBlockWithHeader

func TestStoreBlock_NilBlock(t *testing.T) {
    svc := NewBlockProcessorService(stubLogger{}, &fakeStoreLogger{}, nopStreamReader{}, &fakePublisher{})
    err := svc.StoreBlock(context.Background(), nil)
    var be *apperr.BlockProcessErr
    require.ErrorAs(t, err, &be)
}

func TestStoreBlock_Error(t *testing.T) {
    fl := &fakeStoreLogger{storeErr: errors.New("db down")}
    svc := NewBlockProcessorService(stubLogger{}, fl, nopStreamReader{}, &fakePublisher{})
    tb := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)})
    err := svc.StoreBlock(context.Background(), tb)
    var be *apperr.BlockProcessErr
    require.ErrorAs(t, err, &be)
}

func TestStoreBlock_StoredAndDedup(t *testing.T) {
    // stored true
    fl := &fakeStoreLogger{storeRes: true}
    svc := NewBlockProcessorService(stubLogger{}, fl, nopStreamReader{}, &fakePublisher{})
    require.NoError(t, svc.StoreBlock(context.Background(), types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)})))

    // stored false
    fl2 := &fakeStoreLogger{storeRes: false}
    svc2 := NewBlockProcessorService(stubLogger{}, fl2, nopStreamReader{}, &fakePublisher{})
    require.NoError(t, svc2.StoreBlock(context.Background(), types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)})))
}

func TestReadAndPublishBlock_ContextCanceled(t *testing.T) {
    svc := NewBlockProcessorService(stubLogger{}, &fakeStoreLogger{}, nopStreamReader{}, &fakePublisher{})
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    err := svc.ReadAndPublishBlock(ctx, redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash":"0x01","number":"1","payload":"{}"}})
    require.ErrorIs(t, err, context.Canceled)
}

func TestReadAndPublishBlock_FieldExtractionErrors(t *testing.T) {
    svc := NewBlockProcessorService(stubLogger{}, &fakeStoreLogger{}, nopStreamReader{}, &fakePublisher{})
    // missing hash
    _, _, _, _ = extractAllFields(redis.XMessage{})
    err := svc.ReadAndPublishBlock(context.Background(), redis.XMessage{ID: "0-1", Values: map[string]interface{}{"number":"1","payload":"{}"}})
    var be *apperr.BlockProcessErr
    require.ErrorAs(t, err, &be)

    // invalid number
    err = svc.ReadAndPublishBlock(context.Background(), redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash":"0x01","number":"x","payload":"{}"}})
    require.ErrorAs(t, err, &be)

    // missing payload
    err = svc.ReadAndPublishBlock(context.Background(), redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash":"0x01","number":"1"}})
    require.ErrorAs(t, err, &be)

    // bad payload json
    err = svc.ReadAndPublishBlock(context.Background(), redis.XMessage{ID: "0-1", Values: map[string]interface{}{"hash":"0x01","number":"1","payload":"{" + "}"}})
    require.ErrorAs(t, err, &be)
}

func TestReadAndPublishBlock_SuccessAndBranches(t *testing.T) {
    // successful path
    pub := &fakePublisher{}
    fl := &fakeStoreLogger{pubExists: false, setPubRes: true}
    svc := NewBlockProcessorService(stubLogger{}, fl, nopStreamReader{}, pub)
    payload, _ := MarshalBlockJSON(&entity.Block{Header: entity.Header{Number: 0}})
    msg := redis.XMessage{ID: "42-0", Values: map[string]interface{}{"hash":"0xabc","number":"7","payload": string(payload)}}
    require.NoError(t, svc.ReadAndPublishBlock(context.Background(), msg))
    require.Equal(t, "42-0", pub.lastHeaders["source-message-id"])

    // marker exists path (skip publish)
    pub2 := &fakePublisher{err: errors.New("should not be called")}
    fl2 := &fakeStoreLogger{pubExists: true}
    svc2 := NewBlockProcessorService(stubLogger{}, fl2, nopStreamReader{}, pub2)
    require.NoError(t, svc2.ReadAndPublishBlock(context.Background(), msg))

    // publish error path
    pub3 := &fakePublisher{err: errors.New("kafka down")}
    fl3 := &fakeStoreLogger{}
    svc3 := NewBlockProcessorService(stubLogger{}, fl3, nopStreamReader{}, pub3)
    err := svc3.ReadAndPublishBlock(context.Background(), msg)
    var be *apperr.BlockProcessErr
    require.ErrorAs(t, err, &be)

    // store marker error path
    pub4 := &fakePublisher{}
    fl4 := &fakeStoreLogger{setPubErr: errors.New("redis err")}
    svc4 := NewBlockProcessorService(stubLogger{}, fl4, nopStreamReader{}, pub4)
    err = svc4.ReadAndPublishBlock(context.Background(), msg)
    require.ErrorAs(t, err, &be)
}
