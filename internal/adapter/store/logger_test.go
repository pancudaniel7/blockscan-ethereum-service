package store

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type testLogger struct{ entries []string }

func (l *testLogger) Info(msg string, args ...any)  { l.entries = append(l.entries, "INFO:"+msg) }
func (l *testLogger) Warn(msg string, args ...any)  { l.entries = append(l.entries, "WARN:"+msg) }
func (l *testLogger) Error(msg string, args ...any) { l.entries = append(l.entries, "ERROR:"+msg) }
func (l *testLogger) Debug(msg string, args ...any) { l.entries = append(l.entries, "DEBUG:"+msg) }
func (l *testLogger) Trace(msg string, args ...any) { l.entries = append(l.entries, "TRACE:"+msg) }
func (l *testLogger) Fatal(msg string, args ...any) { l.entries = append(l.entries, "FATAL:"+msg) }

func runMiniRedis(t *testing.T) (*miniredis.Miniredis, string, string) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	host, port, _ := net.SplitHostPort(s.Addr())
	return s, host, port
}

func validLoggerConfig(host, port string) Config {
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
			PublishBlockTTLSeconds:  5,
		},
	}
}

func TestNewBlockLogger_Table(t *testing.T) {
	v := validator.New()
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{name: "invalid_config", cfg: &Config{}, wantErr: true},
		func() struct {
			name    string
			cfg     *Config
			wantErr bool
		} {
			_, h, p := runMiniRedis(t)
			c := validLoggerConfig(h, p)
			return struct {
				name    string
				cfg     *Config
				wantErr bool
			}{name: "valid_config", cfg: &c, wantErr: false}
		}(),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &testLogger{}
			bl, err := NewBlockLogger(logger, nil, v, tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, bl)
			} else {
				require.NoError(t, err)
				require.NotNil(t, bl)
			}
		})
	}
}

func TestStorePublishedBlockHash_Table(t *testing.T) {
	v := validator.New()
	type expectations struct {
		err           bool
		firstCreated  bool
		secondCreated *bool
		checkTTL      bool
	}
	cases := []struct {
		name       string
		ttlSeconds int
		hash       string
		want       expectations
	}{
		{name: "create_then_idempotent", ttlSeconds: 5, hash: "0xabc", want: expectations{err: false, firstCreated: true, secondCreated: boolPtr(false), checkTTL: false}},
		{name: "ttl_set_when_configured", ttlSeconds: 2, hash: "0xdef", want: expectations{err: false, firstCreated: true, secondCreated: nil, checkTTL: true}},
		{name: "empty_hash_rejected", ttlSeconds: 5, hash: "  ", want: expectations{err: true, firstCreated: false, secondCreated: nil, checkTTL: false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, h, p := runMiniRedis(t)
			logger := &testLogger{}
			cfg := validLoggerConfig(h, p)
			cfg.Lock.PublishBlockTTLSeconds = tc.ttlSeconds
			bl, err := NewBlockLogger(logger, nil, v, &cfg)
			require.NoError(t, err)
			ctx := context.Background()
			created, err := bl.StorePublishedBlockHash(ctx, tc.hash)
			if tc.want.err {
				require.Error(t, err)
				require.False(t, created)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want.firstCreated, created)
			if tc.want.secondCreated != nil {
				created2, err := bl.StorePublishedBlockHash(ctx, tc.hash)
				require.NoError(t, err)
				require.Equal(t, *tc.want.secondCreated, created2)
			}
			if tc.want.checkTTL {
				rc := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(h, p)})
				defer rc.Close()
				tag := clusterHashTag(cfg.Streams.Key)
				key := "{" + tag + "}:" + cfg.Lock.DedupPublishBlockPrefix + ":" + tc.hash
				ttl := rc.TTL(ctx, key).Val()
				require.Greater(t, ttl, time.Duration(0))
			}
		})
	}
}

func boolPtr(b bool) *bool { return &b }

type fcallHook struct{ fn func(redis.Cmder) bool }

func (h fcallHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h fcallHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
    return next
}
func (h fcallHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
    return func(ctx context.Context, cmd redis.Cmder) error {
        if cmd.FullName() == "fcall" || cmd.Name() == "fcall" {
            if h.fn != nil {
                if !h.fn(cmd) {
                    return nil
                }
            }
        }
        return next(ctx, cmd)
    }
}

func TestStoreBlock_Table(t *testing.T) {
    v := validator.New()
    mkBlock := func() *entity.Block {
        return &entity.Block{Hash: common.HexToHash("0x1"), Header: entity.Header{Number: 1}}
    }
    cases := []struct{
        name string
        hook func(*redis.Client)
        blk  *entity.Block
        wantStored bool
        wantErr bool
    }{
        {name: "invalid_block_validation", blk: &entity.Block{}, wantStored: false, wantErr: true},
        {name: "do_error", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetErr(errors.New("boom")) }; return false }}) }, blk: mkBlock(), wantErr: true},
        {name: "success_status_1", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal([]interface{}{int64(1), "id-1"}) }; return false }}) }, blk: mkBlock(), wantStored: true},
        {name: "exists_status_0_reason_EXISTS", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal([]interface{}{int64(0), "EXISTS"}) }; return false }}) }, blk: mkBlock(), wantStored: false},
        {name: "xadd_err_status_0_reason_XADD_ERR", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal([]interface{}{int64(0), "XADD_ERR"}) }; return false }}) }, blk: mkBlock(), wantErr: true},
        {name: "unknown_reason_status_0", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal([]interface{}{int64(0), "OTHER"}) }; return false }}) }, blk: mkBlock(), wantErr: true},
        {name: "unexpected_response_type", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal("oops") }; return false }}) }, blk: mkBlock(), wantErr: true},
        {name: "unexpected_status_type", hook: func(c *redis.Client){ c.AddHook(fcallHook{fn: func(cmd redis.Cmder) bool { if cm, ok := cmd.(*redis.Cmd); ok { cm.SetVal([]interface{}{"1", "id"}) }; return false }}) }, blk: mkBlock(), wantErr: true},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            _, h, p := runMiniRedis(t)
            logger := &testLogger{}
            cfg := validLoggerConfig(h, p)
            bl, err := NewBlockLogger(logger, nil, v, &cfg)
            require.NoError(t, err)
            if tc.hook != nil {
                tc.hook(bl.rdb)
            }
            stored, err := bl.StoreBlock(context.Background(), tc.blk)
            if tc.wantErr {
                require.Error(t, err)
                return
            }
            require.NoError(t, err)
            require.Equal(t, tc.wantStored, stored)
        })
    }
}

func TestIsBlockPublished_Table(t *testing.T) {
    v := validator.New()
    cases := []struct{
        name string
        setup func(*testing.T, *BlockLogger)
        hash string
        want bool
        wantErr bool
    }{
        {name: "false_when_not_exists", setup: func(t *testing.T, bl *BlockLogger) {}, hash: "0xabc", want: false},
        {name: "true_after_store_marker", setup: func(t *testing.T, bl *BlockLogger) { _, err := bl.StorePublishedBlockHash(context.Background(), "0xdef"); require.NoError(t, err) }, hash: "0xdef", want: true},
        {name: "empty_hash_rejected", setup: func(t *testing.T, bl *BlockLogger) {}, hash: " ", wantErr: true},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            _, h, p := runMiniRedis(t)
            logger := &testLogger{}
            cfg := validLoggerConfig(h, p)
            bl, err := NewBlockLogger(logger, nil, v, &cfg)
            require.NoError(t, err)
            tc.setup(t, bl)
            got, err := bl.IsBlockPublished(context.Background(), tc.hash)
            if tc.wantErr {
                require.Error(t, err)
                return
            }
            require.NoError(t, err)
            require.Equal(t, tc.want, got)
        })
    }
}

func TestClusterHashTag_Table(t *testing.T) {
	cases := []struct{ name, key, want string }{
		{name: "hashtag", key: "{abc}:stream", want: "abc"},
		{name: "embedded_hashtag", key: "x{abc}y:stream", want: "abc"},
		{name: "no_braces", key: "stream:key", want: "stream:key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, clusterHashTag(c.key))
		})
	}
}
