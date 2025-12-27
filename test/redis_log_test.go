package test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	util "github.com/pancudaniel7/blockscan-ethereum-service/test/util"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestAddBlockLua_Scenarios(t *testing.T) {
	require.NoError(t, util.InitConfig())

	rctr, err := util.InitRedisContainer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rctr.Terminate(context.Background()) })

	rc, err := util.NewRedisClient()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })

	ctx := context.Background()
	streamKey := viper.GetString("redis.streams.key")
	dedupPrefix := viper.GetString("redis.lock.dedup_prefix")
	ttlMs := viper.GetInt("redis.lock.block_ttl_seconds") * 1000

	newSetKey := func() string {
		return fmt.Sprintf("%s:%d", strings.TrimSpace(dedupPrefix), time.Now().UnixNano())
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "transactional rollback on XADD error then success and dedup",
			run: func(t *testing.T) {
				require.NoError(t, util.FlushDB(rc, ctx))
				setKey := newSetKey()
				badRes, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(ttlMs), "*", "field1")
				require.NoError(t, err)
				require.Len(t, badRes, 2)
				require.Equal(t, int64(0), badRes[0].(int64))
				require.Equal(t, "XADD_ERR", badRes[1].(string))
				_, getErr := rc.Get(ctx, setKey).Result()
				require.ErrorIs(t, getErr, redis.Nil)

				okRes, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(ttlMs), "*", "field1", "value1")
				require.NoError(t, err)
				require.Len(t, okRes, 2)
				require.Equal(t, int64(1), okRes[0].(int64))
				_, ok := okRes[1].(string)
				require.True(t, ok)
				require.Equal(t, int64(1), rc.Exists(ctx, setKey).Val())
				require.Equal(t, int64(1), rc.XLen(ctx, streamKey).Val())

				dupRes, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(ttlMs), "*", "field1", "value1")
				require.NoError(t, err)
				require.Len(t, dupRes, 2)
				require.Equal(t, int64(0), dupRes[0].(int64))
				require.Equal(t, "EXISTS", dupRes[1].(string))
				require.Equal(t, int64(1), rc.XLen(ctx, streamKey).Val())
			},
		},
		{
			name: "happy flow and dedup hit",
			run: func(t *testing.T) {
				require.NoError(t, util.FlushDB(rc, ctx))
				setKey := newSetKey()
				res1, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(ttlMs), "*", "field1", "value1", "field2", "value2")
				require.NoError(t, err)
				require.Len(t, res1, 2)
				require.Equal(t, int64(1), res1[0].(int64))
				_, ok := res1[1].(string)
				require.True(t, ok)
				require.Equal(t, int64(1), rc.Exists(ctx, setKey).Val())
				require.Equal(t, int64(1), rc.XLen(ctx, streamKey).Val())

				resDup, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(ttlMs), "*", "field1", "value1")
				require.NoError(t, err)
				require.Len(t, resDup, 2)
				require.Equal(t, int64(0), resDup[0].(int64))
				require.Equal(t, "EXISTS", resDup[1].(string))
				require.Equal(t, int64(1), rc.XLen(ctx, streamKey).Val())
			},
		},
		{
			name: "ttl expiry allows re-add",
			run: func(t *testing.T) {
				require.NoError(t, util.FlushDB(rc, ctx))
				setKey := newSetKey()
				shortTTLms := 150
				res1, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(shortTTLms), "*", "a", "1")
				require.NoError(t, err)
				require.Equal(t, int64(1), res1[0].(int64))
				require.Equal(t, int64(1), rc.XLen(ctx, streamKey).Val())
				time.Sleep(250 * time.Millisecond)
				res2, err := util.CallAddBlockFunction(rc, ctx, setKey, streamKey, strconv.Itoa(shortTTLms), "*", "a", "1")
				require.NoError(t, err)
				require.Equal(t, int64(1), res2[0].(int64))
				require.Equal(t, int64(2), rc.XLen(ctx, streamKey).Val())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
