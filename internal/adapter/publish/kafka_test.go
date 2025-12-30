package publish

import (
	"context"
	stdErrors "errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKgo struct {
	beginErr error
	endErr   error
	prodErrs []error
}

func (f *fakeKgo) BeginTransaction() error                                           { return f.beginErr }
func (f *fakeKgo) EndTransaction(ctx context.Context, a kgo.TransactionEndTry) error { return f.endErr }
func (f *fakeKgo) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	if len(f.prodErrs) == 0 {
		return kgo.ProduceResults{{Record: rs[0], Err: nil}}
	}
	e := f.prodErrs[0]
	f.prodErrs = f.prodErrs[1:]
	return kgo.ProduceResults{{Record: rs[0], Err: e}}
}

func TestNewKafkaPublisher_InvalidConfig(t *testing.T) {
	v := validator.New()
	_, err := NewKafkaPublisher(nil, Config{}, v)
	require.Error(t, err)
}

type testLogger struct{}

func (testLogger) Info(string, ...any)  {}
func (testLogger) Warn(string, ...any)  {}
func (testLogger) Error(string, ...any) {}
func (testLogger) Debug(string, ...any) {}
func (testLogger) Trace(string, ...any) {}
func (testLogger) Fatal(string, ...any) {}

func TestKafkaPublisher_PublishBlock(t *testing.T) {
	v := validator.New()
	base := Config{Brokers: []string{"127.0.0.1:9092"}, Topic: "t", ClientID: "c", MaxRetryAttempts: 1, RetryInitialBackoffMS: 1, RetryMaxBackoffMS: 2, RetryJitter: 0.1, WriteTimeoutSeconds: 1}
	cases := []struct {
		name    string
		cfg     Config
		fk      *fakeKgo
		blk     *entity.Block
		headers map[string]string
		wantErr bool
	}{
		{name: "nil block", cfg: base, fk: &fakeKgo{}, blk: nil, wantErr: true},
		{name: "success no tx", cfg: base, fk: &fakeKgo{}, blk: &entity.Block{Hash: common.HexToHash("0x1"), Header: entity.Header{Number: 1}}, headers: map[string]string{"k": "v"}},
		{name: "success tx", cfg: func() Config { c := base; c.TransactionalID = "tx"; return c }(), fk: &fakeKgo{}, blk: &entity.Block{Hash: common.HexToHash("0x2"), Header: entity.Header{Number: 2}}},
		{name: "retry then fail", cfg: base, fk: &fakeKgo{prodErrs: []error{context.DeadlineExceeded}}, blk: &entity.Block{Hash: common.HexToHash("0x3"), Header: entity.Header{Number: 3}}, wantErr: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			old := newKgoClient
			t.Cleanup(func() { newKgoClient = old })
			newKgoClient = func(opts ...kgo.Opt) (kgoClient, error) { return tc.fk, nil }
			kp, err := NewKafkaPublisher(testLogger{}, tc.cfg, v)
			require.NoError(t, err)
			kp.client = tc.fk
			got := kp.PublishBlock(context.Background(), tc.blk, tc.headers)
			if tc.wantErr {
				require.Error(t, got)
			} else {
				require.NoError(t, got)
			}
		})
	}
}

func TestHelpers(t *testing.T) {
	require.Equal(t, time.Duration(123)*time.Millisecond, millisecondsOrDefault(123, time.Second))
	require.Equal(t, time.Second, millisecondsOrDefault(0, time.Second))
	require.Equal(t, time.Duration(3)*time.Second, secondsOrDefault(3, time.Minute))
	require.Equal(t, time.Minute, secondsOrDefault(0, time.Minute))
}

func TestKafkaPublisher_ShouldRetryAndBuildRecord(t *testing.T) {
	kp := &KafkaPublisher{cfg: Config{Topic: "t"}}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "unknown topic", err: kerr.UnknownTopicOrPartition, want: true},
		{name: "non-retriable", err: stdErrors.New("boom"), want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := kp.shouldRetry(tc.err)
			require.Equal(t, tc.want, got)
		})
	}

	blk := &entity.Block{Hash: common.HexToHash("0xbeef"), Header: entity.Header{Number: 9}}
	rec := kp.buildRecord(blk, []byte("payload"), map[string]string{"x": "y"})
	require.Equal(t, "t", rec.Topic)
	require.Equal(t, blk.Hash.Bytes(), rec.Key)
	var gotNum, gotHash, gotX bool
	for _, h := range rec.Headers {
		if h.Key == "block-number" && string(h.Value) == "9" {
			gotNum = true
		}
		if h.Key == "block-hash" && string(h.Value) == blk.Hash.Hex() {
			gotHash = true
		}
		if h.Key == "x" && string(h.Value) == "y" {
			gotX = true
		}
	}
	require.True(t, gotNum)
	require.True(t, gotHash)
	require.True(t, gotX)
}
