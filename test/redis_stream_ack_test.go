package test

import (
	"context"
	"testing"

	"github.com/pancudaniel7/blockscan-ethereum-service/test/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamConsumerWithAckAndDelete(t *testing.T) {
	require.NoError(t, util.InitConfig())

	rdb, err := util.NewRedisClientFromConfig()
	require.NoError(t, err)
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()
	require.NoError(t, util.FlushDB(rdb, ctx))

	streamKey := "blocks"
	setKey := "block:456"
	ttl := "60000"
	id := "*"
	xAddArgs := []string{"number", "456", "hash", "0xabc123"}

	res, err := util.CallAddBlockFunction(rdb, ctx, setKey, streamKey, ttl, id, xAddArgs...)
	require.NoError(t, err)
	require.Len(t, res, 2)

	messageID, ok := res[1].(string)
	require.True(t, ok)
	require.NotEmpty(t, messageID)

	streamEntries, err := rdb.XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, streamEntries, 1, "Stream should have 1 entry before deletion")

	setVal, err := util.GetValue(rdb, ctx, setKey)
	require.NoError(t, err)
	assert.Equal(t, "1", setVal, "SET key should exist before deletion")

	deletedCount, err := util.XDel(rdb, ctx, streamKey, messageID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedCount, "Should delete 1 message")

	delCount, err := rdb.Del(ctx, setKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), delCount, "Should delete 1 SET key")

	streamEntriesAfter, err := rdb.XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, streamEntriesAfter, 0, "Stream should be empty after deletion")

	_, err = util.GetValue(rdb, ctx, setKey)
	assert.Error(t, err, "SET key should not exist after deletion")
}
