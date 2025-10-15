package test

import (
	"context"
	"testing"

	"github.com/pancudaniel7/blockscan-ethereum-service/test/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddBlockFunctionSetGetAssert(t *testing.T) {
	require.NoError(t, util.InitConfig())

	rdb, err := util.NewRedisClientFromConfig()
	require.NoError(t, err)
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()

	require.NoError(t, util.FlushDB(rdb, ctx))

	setKey := "block:123"
	streamKey := "blocks"
	ttl := "60000"
	id := "*"
	xAddArgs := []string{"number", "123"}

	res, err := util.CallAddBlockFunction(rdb, ctx, setKey, streamKey, ttl, id, xAddArgs...)
	require.NoError(t, err)
	require.Len(t, res, 2)

	setResult, ok := res[0].(int64)
	require.True(t, ok)
	assert.Equal(t, int64(1), setResult, "SETNX should succeed")

	streamID, ok := res[1].(string)
	require.True(t, ok)
	assert.NotEmpty(t, streamID, "Stream ID should be returned")

	val, err := util.GetValue(rdb, ctx, setKey)
	require.NoError(t, err)
	assert.Equal(t, "1", val, "SETNX key should be set to '1'")

	streamEntries, err := rdb.XRange(ctx, streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, streamEntries, 1, "Stream should have 1 entry")
	assert.Equal(t, streamID, streamEntries[0].ID, "Stream ID should match")
	assert.Equal(t, "123", streamEntries[0].Values["number"], "Stream entry should contain block number")
}
