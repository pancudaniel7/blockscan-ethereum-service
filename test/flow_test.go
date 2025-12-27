package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	util "github.com/pancudaniel7/blockscan-ethereum-service/test/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestNewHeadBlockFlow(t *testing.T) {
	require.NoError(t, util.InitConfig())

	anvil, err := util.InitGanacheContainer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = anvil.Terminate(context.Background()) })

	kctr, err := util.InitKafkaContainer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = kctr.Terminate(context.Background()) })

	rctr, err := util.InitRedisContainer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rctr.Terminate(context.Background()) })

	rc, err := util.NewRedisClient()
	require.NoError(t, err)

	t.Cleanup(func() { _ = rc.Close() })
	require.NoError(t, util.FlushDB(rc, context.Background()))

	tests := []struct {
		name  string
		setup func(t *testing.T)
		check func(t *testing.T)
	}{
		{
			name: "scan and log without duplicates",
			setup: func(t *testing.T) {
				name1 := fmt.Sprintf("blockscan-service-test-replica1-%d", time.Now().UnixNano())
				srvRep1, err := util.InitServiceContainer(name1, "blockscan-service:scan-store-replica", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvRep1.Terminate(context.Background()) })

				name2 := fmt.Sprintf("blockscan-service-test-replica2-%d", time.Now().UnixNano())
				srvRep2, err := util.InitServiceContainer(name2, "blockscan-service:scan-store-replica", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvRep2.Terminate(context.Background()) })

				require.NoError(t, util.MineAnvilBlocks(context.Background(), 1))
			},
			check: func(t *testing.T) {
				key := viper.GetString("redis.streams.key")
				deadline := time.Now().Add(30 * time.Second)
				var got bool
				for time.Now().Before(deadline) {
					entries, err := rc.XRange(context.Background(), key, "-", "+").Result()
					if err == nil && len(entries) > 0 {
						got = true
						break
					}
					time.Sleep(500 * time.Millisecond)
				}
				require.True(t, got)
				require.Equal(t, int64(1), rc.XLen(context.Background(), key).Val())
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)
			tt.check(t)
		})
	}
}
