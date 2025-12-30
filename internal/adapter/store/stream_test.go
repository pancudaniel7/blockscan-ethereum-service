package store

import (
    "context"
    "errors"
    "net"
    "sync"
    "testing"
    "time"

    miniredis "github.com/alicebob/miniredis/v2"
    "github.com/go-playground/validator/v10"
    "github.com/redis/go-redis/v9"
    "github.com/stretchr/testify/require"
)

type testLog struct{ entries []string }

func (l *testLog) Info(msg string, args ...any)  { l.entries = append(l.entries, "INFO:"+msg) }
func (l *testLog) Warn(msg string, args ...any)  { l.entries = append(l.entries, "WARN:"+msg) }
func (l *testLog) Error(msg string, args ...any) { l.entries = append(l.entries, "ERROR:"+msg) }
func (l *testLog) Debug(msg string, args ...any) { l.entries = append(l.entries, "DEBUG:"+msg) }
func (l *testLog) Trace(msg string, args ...any) { l.entries = append(l.entries, "TRACE:"+msg) }
func (l *testLog) Fatal(msg string, args ...any) { l.entries = append(l.entries, "FATAL:"+msg) }

func mini(t *testing.T) (*miniredis.Miniredis, string, string) {
    s, err := miniredis.Run()
    require.NoError(t, err)
    t.Cleanup(func() { s.Close() })
    h, p, _ := net.SplitHostPort(s.Addr())
    return s, h, p
}

func validStreamCfg(host, port string) Config {
    return Config{
        Host:               host,
        Port:               port,
        DB:                 0,
        UseTLS:             false,
        PoolSize:           2,
        MaxRetries:         1,
        DialTimeoutSeconds: 1,
        Streams: StreamConfig{
            Key:                     "{blocks}:stream",
            ConsumerGroup:           "cg",
            ConsumerName:            "c1",
            ReadCount:               10,
            ReadBlockTimeoutSeconds: 1,
            ClaimIdleSeconds:        0,
        },
        Lock: LockConfig{
            DedupPrefix:             "block",
            BlockTTLSeconds:         60,
            DedupPublishBlockPrefix: "published",
            PublishBlockTTLSeconds:  0,
        },
    }
}

func TestBlockStream_Table(t *testing.T) {
    v := validator.New()

    type env struct{
        s *miniredis.Miniredis
        rc *redis.Client
        host string
        port string
    }
    mk := func(t *testing.T) env {
        s, h, p := mini(t)
        rc := redis.NewClient(&redis.Options{Addr: s.Addr()})
        t.Cleanup(func(){ _ = rc.Close() })
        return env{s: s, rc: rc, host: h, port: p}
    }

    cases := []struct{
        name string
        run  func(t *testing.T, e env)
    }{
        {
            name: "no_handler_returns_error",
            run: func(t *testing.T, e env) {
                var wg sync.WaitGroup
                lg := &testLog{}
                cfg := validStreamCfg(e.host, e.port)
                bs, err := NewBlockStream(lg, &wg, v, cfg)
                require.NoError(t, err)
                err = bs.StartReadFromStream()
                require.Error(t, err)
            },
        },
        {
            name: "read_and_ack_new_messages",
            run: func(t *testing.T, e env) {
                var wg sync.WaitGroup
                lg := &testLog{}
                cfg := validStreamCfg(e.host, e.port)
                bs, err := NewBlockStream(lg, &wg, v, cfg)
                require.NoError(t, err)
                processed := make(chan struct{}, 1)
                bs.SetHandler(func(ctx context.Context, m redis.XMessage) error {
                    select { case processed <- struct{}{}: default: }
                    return nil
                })
                require.NoError(t, bs.StartReadFromStream())
                t.Cleanup(func() { bs.StopReadFromStream(); wg.Wait() })
                _, err = e.rc.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Streams.Key, Values: map[string]any{"a": "1"}}).Result()
                require.NoError(t, err)
                select {
                case <-processed:
                case <-time.After(2 * time.Second):
                    t.Fatalf("timeout")
                }
                deadline := time.Now().Add(2 * time.Second)
                for time.Now().Before(deadline) {
                    p, perr := e.rc.XPending(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup).Result()
                    if perr == nil && p.Count == 0 {
                        break
                    }
                    time.Sleep(50 * time.Millisecond)
                }
                p, perr := e.rc.XPending(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup).Result()
                require.NoError(t, perr)
                require.Equal(t, int64(0), p.Count)
            },
        },
        {
            name: "handler_error_leaves_pending",
            run: func(t *testing.T, e env) {
                var wg sync.WaitGroup
                lg := &testLog{}
                cfg := validStreamCfg(e.host, e.port)
                _, err := e.rc.XGroupCreateMkStream(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup, "0").Result()
                require.NoError(t, err)
                _, err = e.rc.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Streams.Key, Values: map[string]any{"k": "v"}}).Result()
                require.NoError(t, err)
                _, err = e.rc.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: cfg.Streams.ConsumerGroup, Consumer: cfg.Streams.ConsumerName, Streams: []string{cfg.Streams.Key, ">"}, Count: 1}).Result()
                require.NoError(t, err)
                bs, err := NewBlockStream(lg, &wg, v, cfg)
                require.NoError(t, err)
                ran := make(chan struct{}, 1)
                bs.SetHandler(func(ctx context.Context, m redis.XMessage) error {
                    select { case ran <- struct{}{}: default: }
                    return errors.New("fail")
                })
                require.NoError(t, bs.StartReadFromStream())
                select {
                case <-ran:
                case <-time.After(2 * time.Second):
                    t.Fatalf("timeout")
                }
                bs.StopReadFromStream()
                wg.Wait()
                p, perr := e.rc.XPending(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup).Result()
                require.NoError(t, perr)
                require.Equal(t, int64(1), p.Count)
            },
        },
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            e := mk(t)
            tc.run(t, e)
        })
    }
}

func TestBlockStream_EnsureGroup_Table(t *testing.T) {
    v := validator.New()
    t.Run("create_and_idempotent_when_exists", func(t *testing.T) {
        _, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        require.NoError(t, bs.ensureConsumerGroup(context.Background()))
        require.NoError(t, bs.ensureConsumerGroup(context.Background()))
    })
    t.Run("ensure_with_retry_times_out_on_ctx", func(t *testing.T) {
        s, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        s.Close()
        ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
        defer cancel()
        err = bs.ensureGroupWithRetry(ctx)
        require.Error(t, err)
    })
}

func TestBlockStream_ReadNew_And_DrainPending_Paths_Table(t *testing.T) {
    v := validator.New()
    t.Run("readNew_creates_group_on_nogroup", func(t *testing.T) {
        s, host, port := mini(t)
        rc := redis.NewClient(&redis.Options{Addr: s.Addr()})
        defer rc.Close()
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        err = bs.readNew(context.Background(), cfg.Streams.ConsumerName, cfg.Streams.ReadCount, 100*time.Millisecond)
        require.NoError(t, err)
        _, gErr := rc.XInfoGroups(context.Background(), cfg.Streams.Key).Result()
        require.NoError(t, gErr)
    })
    t.Run("drain_pending_no_messages", func(t *testing.T) {
        _, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        require.NoError(t, bs.ensureConsumerGroup(context.Background()))
        err = bs.drainPending(context.Background(), cfg.Streams.ConsumerName, cfg.Streams.ReadCount)
        require.NoError(t, err)
    })
}

func TestBlockStream_ReclaimStale_Table(t *testing.T) {
    v := validator.New()
    t.Run("no_messages_returns_nil", func(t *testing.T) {
        _, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        require.NoError(t, bs.ensureConsumerGroup(context.Background()))
        err = bs.reclaimStale(context.Background(), cfg.Streams.ConsumerName, cfg.Streams.ReadCount, 0)
        require.NoError(t, err)
    })
    t.Run("claims_and_processes_stale_messages", func(t *testing.T) {
        s, host, port := mini(t)
        rc := redis.NewClient(&redis.Options{Addr: s.Addr()})
        defer rc.Close()
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        _, err = rc.XGroupCreateMkStream(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup, "0").Result()
        require.NoError(t, err)
        id, err := rc.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Streams.Key, Values: map[string]any{"k": "v"}}).Result()
        require.NoError(t, err)
        _, err = rc.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: cfg.Streams.ConsumerGroup, Consumer: "other", Streams: []string{cfg.Streams.Key, ">"}, Count: 1}).Result()
        require.NoError(t, err)
        processed := make(chan string, 1)
        bs.SetHandler(func(ctx context.Context, m redis.XMessage) error {
            select { case processed <- m.ID: default: }
            return nil
        })
        err = bs.reclaimStale(context.Background(), cfg.Streams.ConsumerName, cfg.Streams.ReadCount, 0)
        require.NoError(t, err)
        select {
        case got := <-processed:
            require.Equal(t, id, got)
        case <-time.After(2 * time.Second):
            t.Fatalf("timeout")
        }
    })
}

func TestBlockStream_Ack_Stop_Table(t *testing.T) {
    v := validator.New()
    t.Run("ack_empty_id_is_noop", func(t *testing.T) {
        _, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        bs.ackMessage("")
    })
    t.Run("ack_valid_id_and_retry_on_error", func(t *testing.T) {
        s, host, port := mini(t)
        rc := redis.NewClient(&redis.Options{Addr: s.Addr()})
        defer rc.Close()
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        _, err = rc.XGroupCreateMkStream(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup, "0").Result()
        require.NoError(t, err)
        id, err := rc.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Streams.Key, Values: map[string]any{"k": "v"}}).Result()
        require.NoError(t, err)
        _, err = rc.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: cfg.Streams.ConsumerGroup, Consumer: cfg.Streams.ConsumerName, Streams: []string{cfg.Streams.Key, ">"}, Count: 1}).Result()
        require.NoError(t, err)
        bs.ackMessage(id)
        p, perr := rc.XPending(context.Background(), cfg.Streams.Key, cfg.Streams.ConsumerGroup).Result()
        require.NoError(t, perr)
        require.Equal(t, int64(0), p.Count)
        s.Close()
        bs.ackMessage("1-0")
    })
    t.Run("stop_when_not_started_is_noop", func(t *testing.T) {
        _, host, port := mini(t)
        lg := &testLog{}
        var wg sync.WaitGroup
        cfg := validStreamCfg(host, port)
        bs, err := NewBlockStream(lg, &wg, v, cfg)
        require.NoError(t, err)
        bs.StopReadFromStream()
    })
}

func TestIsNoGroupErr_Table(t *testing.T) {
    cases := []struct{ name string; err error; want bool }{
        {name: "nogroup_upper", err: errors.New("NOGROUP foo"), want: true},
        {name: "nogroup_mixed", err: errors.New("noGroup bar"), want: true},
        {name: "nil_err", err: nil, want: false},
        {name: "other", err: errors.New("x"), want: false},
    }
    for _, c := range cases {
        t.Run(c.name, func(t *testing.T) {
            require.Equal(t, c.want, isNoGroupErr(c.err))
        })
    }
}
