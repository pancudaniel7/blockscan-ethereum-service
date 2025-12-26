package scan

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
	"github.com/stretchr/testify/require"
)

func TestEthereumScanner_connectClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name                  string
		attemptsBeforeSuccess int
		ctxTimeout            time.Duration
		wantErr               bool
	}{
		{
			name:                  "succeeds_after_retries",
			attemptsBeforeSuccess: 3,
			ctxTimeout:            3 * time.Second,
			wantErr:               false,
		},
		{
			name:                  "context_deadline",
			attemptsBeforeSuccess: 100,
			ctxTimeout:            50 * time.Millisecond,
			wantErr:               true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scanner := &EthereumScanner{
				log:    noopLogger{},
				config: &Config{WebSocketsURL: "ws://ignored"},
			}

			var attempts int32
			client := &fakeEthereumClient{}
			scanner.newClient = func(ctx context.Context) (ethereumClient, error) {
				if atomic.AddInt32(&attempts, 1) < int32(tc.attemptsBeforeSuccess) {
					return nil, errors.New("dial failed")
				}
				return client, nil
			}

			ctx, cancel := context.WithTimeout(context.Background(), tc.ctxTimeout)
			defer cancel()

			got, err := scanner.connectClient(ctx)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, client, got)
			require.Equal(t, int32(tc.attemptsBeforeSuccess), attempts)
		})
	}
}

func TestEthereumScanner_scanNewHeadsFailures(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, s *EthereumScanner, reconnect chan struct{})
	}{
		{
			name: "subscribe_failure_triggers_reconnect",
			setup: func(t *testing.T, s *EthereumScanner, reconnect chan struct{}) {
				s.handler = func(context.Context, *types.Block) error { return nil }
				var connects atomic.Int32
				s.newClient = func(ctx context.Context) (ethereumClient, error) {
					if connects.Add(1) == 2 {
						closeOnce(reconnect)
					}
					return &fakeEthereumClient{
						subscribeFn: func(context.Context, chan<- *types.Header) (ethereum.Subscription, error) {
							return nil, errors.New("subscribe failure")
						},
						closeFn: func() {},
					}, nil
				}
			},
		},
		{
			name: "block_fetch_failure_forces_reconnect",
			setup: func(t *testing.T, s *EthereumScanner, reconnect chan struct{}) {
				var connects atomic.Int32
				s.handler = func(context.Context, *types.Block) error { return nil }
				s.newClient = func(ctx context.Context) (ethereumClient, error) {
					attempt := connects.Add(1)
					if attempt == 2 {
						closeOnce(reconnect)
					}
					return &fakeEthereumClient{
						subscribeFn: func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
							go func() {
								ch <- &types.Header{Number: big.NewInt(5)}
							}()
							return newFakeSubscription(), nil
						},
						blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
							if attempt == 1 {
								return nil, errors.New("fetch err")
							}
							return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(5)}), nil
						},
						closeFn: func() {},
					}, nil
				}
			},
		},
		{
			name: "handler_failure_triggers_reconnect",
			setup: func(t *testing.T, s *EthereumScanner, reconnect chan struct{}) {
				var connects atomic.Int32
				var handled atomic.Bool
				s.handler = func(context.Context, *types.Block) error {
					if handled.CompareAndSwap(false, true) {
						return errors.New("handler boom")
					}
					return nil
				}
				s.newClient = func(ctx context.Context) (ethereumClient, error) {
					attempt := connects.Add(1)
					if attempt == 2 {
						closeOnce(reconnect)
					}
					return &fakeEthereumClient{
						subscribeFn: func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
							go func() {
								ch <- &types.Header{Number: big.NewInt(9)}
							}()
							return newFakeSubscription(), nil
						},
						blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
							return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(9)}), nil
						},
						closeFn: func() {},
					}, nil
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{WebSocketsURL: "ws://ignored"}
			s := &EthereumScanner{
				log:    noopLogger{},
				config: cfg,
			}

			reconnect := make(chan struct{})
			tc.setup(t, s, reconnect)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				s.scanNewHeads(ctx)
				close(done)
			}()

			select {
			case <-reconnect:
			case <-time.After(time.Second):
				t.Fatal("scanner did not reconnect")
			}

			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("scanNewHeads did not exit")
			}
		})
	}
}

func TestEthereumScanner_scanNewHeads_ImmediateCancel(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	s := &EthereumScanner{log: logger, config: &Config{WebSocketsURL: "ws://ignored"}, handler: func(context.Context, *types.Block) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.scanNewHeads(ctx)
	msgs := logger.messages()
	var saw bool
	for _, m := range msgs {
		if m == "Stopping Ethereum scan..." {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("expected stop trace, got %v", msgs)
	}
}

func TestEthereumScanner_scanNewHeads_SubscriptionErrorChannelReconnects(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored"}
	s := &EthereumScanner{
		log:    noopLogger{},
		config: cfg,
		handler: func(context.Context, *types.Block) error {
			return nil
		},
	}
	var connects atomic.Int32
	reconnected := make(chan struct{})
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		if connects.Add(1) == 2 {
			closeOnce(reconnected)
		}
		return &fakeEthereumClient{
			subscribeFn: func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
				fs := newFakeSubscription()
				go func() {
					defer func() { _ = recover() }()
					fs.errCh <- errors.New("subscription error")
				}()
				return fs, nil
			},
			closeFn: func() {},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.scanNewHeads(ctx)
		close(done)
	}()

	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("scanner did not reconnect on subscription error")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scanNewHeads did not exit after cancel")
	}
}

func TestEthereumScanner_scanNewHeads_ConnectErrorLogsAndRetries(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored"}
	logger := &levelCountingLogger{}
	s := &EthereumScanner{
		log:           logger,
		config:        cfg,
		handler:       func(context.Context, *types.Block) error { return nil },
		dialRetryOpts: []pattern.RetryOption{pattern.WithMaxAttempts(1)},
	}
	s.newClient = func(context.Context) (ethereumClient, error) { return nil, errors.New("dial failed") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.scanNewHeads(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		if logger.errors.Load() > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no error log observed for connect failure")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scanNewHeads did not exit after cancel")
	}
}

func TestEthereumScanner_scanNewHeads_SequentialCatchUp(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored"}
	s := &EthereumScanner{
		log:           noopLogger{},
		config:        cfg,
		handler:       func(context.Context, *types.Block) error { return nil },
		lastProcessed: 3,
	}
	var gotFetches []uint64
	fetchedDone := make(chan struct{})
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return &fakeEthereumClient{
			subscribeFn: func(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
				fs := newFakeSubscription()
				go func() { ch <- &types.Header{Number: big.NewInt(5)} }()
				return fs, nil
			},
			blockByNumberFn: func(_ context.Context, n *big.Int) (*types.Block, error) {
				gotFetches = append(gotFetches, n.Uint64())
				if n.Uint64() == 5 {
					closeOnce(fetchedDone)
				}
				return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).Set(n)}), nil
			},
			closeFn: func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.scanNewHeads(ctx)
		close(done)
	}()

	select {
	case <-fetchedDone:
	case <-time.After(2 * time.Second):
		t.Fatal("catch-up fetches did not complete")
	}
	cancel()
	<-done
	require.Equal(t, []uint64{4, 5}, gotFetches)
	require.Equal(t, uint64(5), s.lastProcessed)
}

func TestEthereumScanner_scanFinalized(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, s *EthereumScanner, reconnect chan struct{}, processed chan uint64)
	}{
		{
			name: "block_fetch_failure_then_success",
			setup: func(t *testing.T, s *EthereumScanner, reconnect chan struct{}, processed chan uint64) {
				var connects atomic.Int32
				s.handler = func(_ context.Context, b *types.Block) error {
					processed <- b.NumberU64()
					return nil
				}
				s.newClient = func(ctx context.Context) (ethereumClient, error) {
					attempt := connects.Add(1)
					if attempt == 2 {
						closeOnce(reconnect)
					}
					return &fakeEthereumClient{
						headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
							return &types.Header{Number: big.NewInt(12)}, nil
						},
						blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
							if attempt == 1 {
								return nil, errors.New("fetch")
							}
							return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(12)}), nil
						},
						blockNumberFn: func(context.Context) (uint64, error) {
							return 12, nil
						},
						closeFn: func() {},
					}, nil
				}
			},
		},
		{
			name: "finalized_head_resolution_error",
			setup: func(t *testing.T, s *EthereumScanner, reconnect chan struct{}, processed chan uint64) {
				var connects atomic.Int32
				s.handler = func(_ context.Context, b *types.Block) error {
					processed <- b.NumberU64()
					return nil
				}
				s.newClient = func(ctx context.Context) (ethereumClient, error) {
					attempt := connects.Add(1)
					if attempt == 2 {
						closeOnce(reconnect)
					}
					return &fakeEthereumClient{
						headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
							if attempt == 1 {
								return nil, errors.New("finalized tag unsupported")
							}
							return &types.Header{Number: big.NewInt(20)}, nil
						},
						blockNumberFn: func(context.Context) (uint64, error) {
							if attempt == 1 {
								return 0, errors.New("latest failed")
							}
							return 20, nil
						},
						blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
							return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(20)}), nil
						},
						closeFn: func() {},
					}, nil
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{
				WebSocketsURL:          "ws://ignored",
				FinalizedBlocks:        true,
				FinalizedPollDelay:     1,
				FinalizedConfirmations: 1,
			}
			s := &EthereumScanner{
				log:    noopLogger{},
				config: cfg,
			}

			reconnect := make(chan struct{})
			processed := make(chan uint64, 1)
			tc.setup(t, s, reconnect, processed)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				s.scanFinalized(ctx)
				close(done)
			}()

			select {
			case <-reconnect:
			case <-time.After(2 * time.Second):
				t.Fatal("finalized scanner failed to reconnect")
			}

			select {
			case <-processed:
			case <-time.After(2 * time.Second):
				t.Fatal("finalized scanner did not process block")
			}

			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("scanFinalized did not exit")
			}
		})
	}
}

func TestEthereumScanner_scanFinalized_HandlerFailureReconnects(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	s := &EthereumScanner{log: noopLogger{}, config: cfg}
	var connects atomic.Int32
	var handled atomic.Bool
	processed := make(chan struct{}, 1)
	reconnected := make(chan struct{})
	s.handler = func(context.Context, *types.Block) error {
		if handled.CompareAndSwap(false, true) {
			return errors.New("handler fail")
		}
		closeOnce(processed)
		return nil
	}
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		attempt := connects.Add(1)
		if attempt == 2 {
			closeOnce(reconnected)
		}
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				return &types.Header{Number: big.NewInt(10)}, nil
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 10, nil },
			blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
				return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(10)}), nil
			},
			closeFn: func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()
	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("did not reconnect after handler failure")
	}
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("did not process after reconnect")
	}
	cancel()
	<-done
}

func TestEthereumScanner_scanFinalized_CancelWhileFetching(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	s := &EthereumScanner{log: noopLogger{}, config: cfg}
	var connects atomic.Int32
	s.handler = func(context.Context, *types.Block) error { return nil }
	gate := make(chan struct{})
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		connects.Add(1)
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				return &types.Header{Number: big.NewInt(7)}, nil
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 7, nil },
			blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
				<-gate
				return nil, errors.New("fetch canceled")
			},
			closeFn: func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	close(gate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scanFinalized did not exit after cancel during fetch")
	}
}

func TestEthereumScanner_scanFinalized_IdlePollThenCancel(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 0, FinalizedConfirmations: 1}
	s := &EthereumScanner{log: noopLogger{}, config: cfg}
	s.lastProcessed = 10
	s.handler = func(context.Context, *types.Block) error { return nil }
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				return &types.Header{Number: big.NewInt(10)}, nil
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 10, nil },
			closeFn:       func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done
}

func TestEthereumScanner_scanFinalized_DrainFetchUsesTimeout(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	s := &EthereumScanner{log: noopLogger{}, config: cfg}
	s.handler = func(context.Context, *types.Block) error { return nil }
	backlogReady := make(chan struct{})
	var dial atomic.Int32
	var call atomic.Int32
	var hadDeadline atomic.Bool
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		d := dial.Add(1)
		if d == 1 {
			return &fakeEthereumClient{
				headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
					closeOnce(backlogReady)
					return &types.Header{Number: big.NewInt(3)}, nil
				},
				blockNumberFn: func(context.Context) (uint64, error) { return 3, nil },
				blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
					if call.Add(1) == 1 {
						return nil, errors.New("fetch fail")
					}
					return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(3)}), nil
				},
				closeFn: func() {},
			}, nil
		}
		return &fakeEthereumClient{
			blockByNumberFn: func(ctx context.Context, n *big.Int) (*types.Block, error) {
				if _, ok := ctx.Deadline(); ok {
					hadDeadline.Store(true)
				}
				return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).Set(n)}), nil
			},
			closeFn: func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()
	<-backlogReady
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("finalized drain path did not exit")
	}
	require.True(t, hadDeadline.Load())
}

func TestEthereumScanner_scanFinalized_StopRequestedWhileResolving(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	s := &EthereumScanner{log: logger, config: cfg}
	gate := make(chan struct{})
	s.handler = func(context.Context, *types.Block) error { return nil }
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				<-gate
				return nil, errors.New("finalized unsupported")
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 0, errors.New("latest fail") },
			closeFn:       func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(gate)
	<-done
	msgs := logger.messages()
	var saw bool
	for _, m := range msgs {
		if m == "Stop requested while resolving finalized head; entering drain if needed" {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("expected stop-resolving trace, got %v", msgs)
	}
}

func TestEthereumScanner_scanFinalized_ConnectErrorThenSuccess(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	logger := &levelCountingLogger{}
	s := &EthereumScanner{log: logger, config: cfg}
	var connects atomic.Int32
	reconnected := make(chan struct{})
	processed := make(chan struct{}, 1)
	s.handler = func(context.Context, *types.Block) error { closeOnce(processed); return nil }
	s.dialRetryOpts = []pattern.RetryOption{pattern.WithMaxAttempts(1)}
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		attempt := connects.Add(1)
		if attempt == 1 {
			return nil, errors.New("connect fail")
		}
		if attempt == 2 {
			closeOnce(reconnected)
		}
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				return &types.Header{Number: big.NewInt(1)}, nil
			},
			blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
				return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)}), nil
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 1, nil },
			closeFn:       func() {},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.scanFinalized(ctx); close(done) }()

	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("did not reconnect after connect error")
	}
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("did not process block after reconnect")
	}
	cancel()
	<-done
	require.Greater(t, logger.warns.Load(), int32(0))
}

func TestEthereumScanner_connectClient_NoFactory(t *testing.T) {
	t.Parallel()
	s := &EthereumScanner{log: noopLogger{}, config: &Config{WebSocketsURL: "ws://ignored"}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := s.connectClient(ctx)
	require.Error(t, err)
}

func TestEthereumScanner_fetchBlockByNumber_UsesTimeout(t *testing.T) {
	t.Parallel()
	s := &EthereumScanner{log: noopLogger{}, config: &Config{WebSocketsURL: "ws://ignored"}}
	var hadDeadline atomic.Bool
	client := &fakeEthereumClient{
		blockByNumberFn: func(ctx context.Context, n *big.Int) (*types.Block, error) {
			if _, ok := ctx.Deadline(); ok {
				hadDeadline.Store(true)
			}
			return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).Set(n)}), nil
		},
	}
	blk, err := s.fetchBlockByNumber(context.Background(), client, 42)
	require.NoError(t, err)
	require.NotNil(t, blk)
	require.True(t, hadDeadline.Load())
}

func TestEthereumScanner_SetHandler(t *testing.T) {
	t.Parallel()
	s := &EthereumScanner{}
	called := atomic.Bool{}
	s.SetHandler(func(context.Context, *types.Block) error { called.Store(true); return nil })
	require.NotNil(t, s.handler)
}

func TestNewEthereumScanner_Validation(t *testing.T) {
	t.Parallel()
	v := validator.New()
	wg := &sync.WaitGroup{}

	errLogger := &levelCountingLogger{}
	_, err := NewEthereumScanner(errLogger, wg, &Config{}, v)
	require.Error(t, err)
	require.Greater(t, errLogger.errors.Load(), int32(0))

	okLogger := &levelCountingLogger{}
	s, err := NewEthereumScanner(okLogger, wg, &Config{WebSocketsURL: "ws://localhost:0"}, v)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.NotNil(t, s.newClient)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, _ = s.newClient(ctx)
}

func TestEthereumScanner_currentFinalizedNumber(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		client          *fakeEthereumClient
		finalizedBlocks uint64
		want            uint64
		wantErr         bool
	}{
		{
			name: "uses_finalized_tag_when_available",
			client: &fakeEthereumClient{
				headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
					return &types.Header{Number: big.NewInt(77)}, nil
				},
			},
			want: 77,
		},
		{
			name: "falls_back_to_latest_minus_confirmations",
			client: &fakeEthereumClient{
				headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
					return nil, errors.New("unsupported")
				},
				blockNumberFn: func(context.Context) (uint64, error) {
					return 100, nil
				},
			},
			finalizedBlocks: 10,
			want:            90,
		},
		{
			name: "fails_when_latest_below_confirmations",
			client: &fakeEthereumClient{
				headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
					return nil, errors.New("unsupported")
				},
				blockNumberFn: func(context.Context) (uint64, error) {
					return 5, nil
				},
			},
			finalizedBlocks: 10,
			wantErr:         true,
		},
		{
			name: "propagates_latest_error_when_finalized_tag_missing",
			client: &fakeEthereumClient{
				headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
					return nil, errors.New("unsupported")
				},
				blockNumberFn: func(context.Context) (uint64, error) {
					return 0, errors.New("latest err")
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{FinalizedConfirmations: tc.finalizedBlocks}
			s := &EthereumScanner{config: cfg}

			got, err := s.currentFinalizedNumber(context.Background(), tc.client)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEthereumScanner_handleBlock(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		handlerErr    error
		startLastProc uint64
		blockNumber   uint64
		wantLastProc  uint64
		wantErr       bool
	}{
		{
			name:          "updates_last_processed_on_success",
			startLastProc: 10,
			blockNumber:   15,
			wantLastProc:  15,
		},
		{
			name:          "keeps_last_processed_on_error",
			handlerErr:    errors.New("boom"),
			startLastProc: 20,
			blockNumber:   30,
			wantLastProc:  20,
			wantErr:       true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := &EthereumScanner{
				lastProcessed: tc.startLastProc,
				handler: func(context.Context, *types.Block) error {
					return tc.handlerErr
				},
				log: noopLogger{},
			}

			block := types.NewBlockWithHeader(&types.Header{Number: big.NewInt(int64(tc.blockNumber))})
			err := s.handleBlock(context.Background(), block)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantLastProc, s.lastProcessed)
		})
	}
}

func TestEthereumScanner_StartScanning_HandlerMissing(t *testing.T) {
	t.Parallel()
	s := &EthereumScanner{
		log:    noopLogger{},
		wg:     &sync.WaitGroup{},
		config: &Config{WebSocketsURL: "ws://ignored"},
	}

	err := s.StartScanning()
	require.Error(t, err)
}

func TestEthereumScanner_StartScanning_AlreadyRunning(t *testing.T) {
	t.Parallel()
	s := &EthereumScanner{
		log:     noopLogger{},
		wg:      &sync.WaitGroup{},
		config:  &Config{WebSocketsURL: "ws://ignored"},
		handler: func(context.Context, *types.Block) error { return nil },
		running: true,
		cancel:  func() {},
	}

	err := s.StartScanning()
	require.Error(t, err)
}

func TestEthereumScanner_StartScanning_LaunchesNewHeads(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored"}
	wg := &sync.WaitGroup{}
	subscribed := make(chan struct{})
	s := &EthereumScanner{
		log:    noopLogger{},
		wg:     wg,
		config: cfg,
		handler: func(context.Context, *types.Block) error {
			return nil
		},
	}

	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return &fakeEthereumClient{
			subscribeFn: func(subCtx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
				closeOnce(subscribed)
				go func() {
					<-subCtx.Done()
					close(ch)
				}()
				return newFakeSubscription(), nil
			},
			closeFn: func() {},
		}, nil
	}

	require.NoError(t, s.StartScanning())
	t.Cleanup(func() {
		s.StopScanning()
	})

	select {
	case <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("scan goroutine did not subscribe to new heads")
	}

	s.StopScanning()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scanner goroutine did not exit")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	require.False(t, s.running)
	require.Nil(t, s.cancel)
}

func TestEthereumScanner_StartScanning_LaunchesFinalized(t *testing.T) {
	t.Parallel()
	cfg := &Config{WebSocketsURL: "ws://ignored", FinalizedBlocks: true, FinalizedPollDelay: 1, FinalizedConfirmations: 1}
	wg := &sync.WaitGroup{}
	started := make(chan struct{})
	s := &EthereumScanner{
		log:     noopLogger{},
		wg:      wg,
		config:  cfg,
		handler: func(context.Context, *types.Block) error { return nil },
	}
	var called atomic.Bool
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return &fakeEthereumClient{
			headerByNumberFn: func(context.Context, *big.Int) (*types.Header, error) {
				if called.CompareAndSwap(false, true) {
					closeOnce(started)
				}
				return &types.Header{Number: big.NewInt(1)}, nil
			},
			blockNumberFn: func(context.Context) (uint64, error) { return 1, nil },
			blockByNumberFn: func(context.Context, *big.Int) (*types.Block, error) {
				return types.NewBlockWithHeader(&types.Header{Number: big.NewInt(1)}), nil
			},
			closeFn: func() {},
		}, nil
	}

	require.NoError(t, s.StartScanning())
	t.Cleanup(func() { s.StopScanning() })

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("finalized scanner did not start")
	}

	s.StopScanning()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("finalized scanner goroutine did not exit")
	}
}

func TestEthereumScanner_StopScanning_NoActiveContext(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	s := &EthereumScanner{
		log: logger,
	}

	s.StopScanning()

	require.Equal(t, []string{"Ethereum scanning stopped"}, logger.messages())
}

func TestEthereumScanner_StopScanning_CancelsOnce(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	var canceled atomic.Int32
	s := &EthereumScanner{
		log: logger,
	}
	s.cancel = func() {
		canceled.Add(1)
	}

	s.StopScanning()
	s.StopScanning()

	require.Equal(t, int32(1), canceled.Load())
	s.mu.Lock()
	require.Nil(t, s.cancel)
	s.mu.Unlock()
	require.Equal(t, []string{
		"Cancelling Ethereum scanning...",
		"Ethereum scanning stopped",
		"Ethereum scanning stopped",
	}, logger.messages())
}

type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Trace(string, ...any) {}
func (noopLogger) Fatal(string, ...any) {}

type recordingLogger struct {
	mu      sync.Mutex
	entries []string
}

func (*recordingLogger) Info(string, ...any)  {}
func (*recordingLogger) Warn(string, ...any)  {}
func (*recordingLogger) Error(string, ...any) {}
func (*recordingLogger) Debug(string, ...any) {}
func (r *recordingLogger) Trace(msg string, _ ...any) {
	r.mu.Lock()
	r.entries = append(r.entries, msg)
	r.mu.Unlock()
}
func (*recordingLogger) Fatal(string, ...any) {}

func (r *recordingLogger) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.entries))
	copy(out, r.entries)
	return out
}

type levelCountingLogger struct {
	errors atomic.Int32
	warns  atomic.Int32
}

func (l *levelCountingLogger) Info(string, ...any)  {}
func (l *levelCountingLogger) Warn(string, ...any)  { l.warns.Add(1) }
func (l *levelCountingLogger) Error(string, ...any) { l.errors.Add(1) }
func (l *levelCountingLogger) Debug(string, ...any) {}
func (l *levelCountingLogger) Trace(string, ...any) {}
func (l *levelCountingLogger) Fatal(string, ...any) {}

type fakeEthereumClient struct {
	subscribeFn      func(context.Context, chan<- *types.Header) (ethereum.Subscription, error)
	blockByNumberFn  func(context.Context, *big.Int) (*types.Block, error)
	headerByNumberFn func(context.Context, *big.Int) (*types.Header, error)
	blockNumberFn    func(context.Context) (uint64, error)
	closeFn          func()
}

func (f *fakeEthereumClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	if f.subscribeFn != nil {
		return f.subscribeFn(ctx, ch)
	}
	return nil, errors.New("subscribe not implemented")
}

func (f *fakeEthereumClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	if f.blockByNumberFn != nil {
		return f.blockByNumberFn(ctx, number)
	}
	return nil, errors.New("blockByNumber not implemented")
}

func (f *fakeEthereumClient) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	if f.headerByNumberFn != nil {
		return f.headerByNumberFn(ctx, number)
	}
	return nil, errors.New("headerByNumber not implemented")
}

func (f *fakeEthereumClient) BlockNumber(ctx context.Context) (uint64, error) {
	if f.blockNumberFn != nil {
		return f.blockNumberFn(ctx)
	}
	return 0, errors.New("blockNumber not implemented")
}

func (f *fakeEthereumClient) Close() {
	if f.closeFn != nil {
		f.closeFn()
	}
}

type fakeSubscription struct {
	errCh        chan error
	unsubscribe  func()
	once         sync.Once
	unsubscribed atomic.Bool
}

func newFakeSubscription() *fakeSubscription {
	return &fakeSubscription{
		errCh: make(chan error),
	}
}

func (f *fakeSubscription) Err() <-chan error {
	return f.errCh
}

func (f *fakeSubscription) Unsubscribe() {
	if f.unsubscribe != nil && f.unsubscribed.CompareAndSwap(false, true) {
		f.unsubscribe()
	}
	f.once.Do(func() {
		close(f.errCh)
	})
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
