package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	util "github.com/pancudaniel7/blockscan-ethereum-service/test/pkg"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// FPEnvBeforePublish activates the failpoint that crashes a replica right
// before publishing a block. It validates that another replica can publish
// without duplicates and the stream stays clean of pending messages.
const FPEnvBeforePublish = "github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase/fail-before-publish=return(true)"

// FPEnvBeforeAck activates the failpoint that crashes a replica immediately
// before acknowledging the stream message. It validates that pending entries
// can be re-claimed and processed by a healthy replica without duplicate publication.
const FPEnvBeforeAck = "github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/store/fail-before-ack=return(true)"

// TestNewHeadBlockFlow exercises end-to-end flow with replicas under various
// failpoint-induced crash scenarios to validate idempotency and dedup logic.

func TestNewHeadBlockFlow(t *testing.T) {
	require.NoError(t, util.InitConfig())
	util.EnsureServiceImageBuilt(t)
	var rc *redis.Client

	tests := []struct {
		name  string
		setup func(t *testing.T)
		check func(t *testing.T)
	}{
		{
			name: "successfully scan log and publish without duplicates",
			setup: func(t *testing.T) {
				name1 := fmt.Sprintf("blockscan-service-test-replica1-%d", time.Now().UnixNano())
				srvRep1, err := util.InitServiceContainer(name1, "", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvRep1.Terminate(context.Background()) })

				name2 := fmt.Sprintf("blockscan-service-test-replica2-%d", time.Now().UnixNano())
				srvRep2, err := util.InitServiceContainer(name2, "", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvRep2.Terminate(context.Background()) })

				require.NoError(t, util.MineAnvilBlocks(context.Background(), 1))
			},
			check: func(t *testing.T) {
				key := viper.GetString("redis.streams.key")
				deadline := time.Now().Add(30 * time.Second)
				pi := viper.GetInt("test.poll_interval_ms")
				if pi <= 0 {
					pi = 300
				}
				pollInterval := time.Duration(pi) * time.Millisecond
				var got bool
				for time.Now().Before(deadline) {
					entries, err := rc.XRange(context.Background(), key, "-", "+").Result()
					if err == nil && len(entries) > 0 {
						got = true
						break
					}
					time.Sleep(pollInterval)
				}
				require.True(t, got)
				require.Equal(t, int64(1), rc.XLen(context.Background(), key).Val())
			},
		},
		{
			name: "successfully scan log and publish where one replica fails before publishing",
			setup: func(t *testing.T) {
				fp := FPEnvBeforePublish
				nameFail := fmt.Sprintf("blockscan-service-fp-pre-publish-%d", time.Now().UnixNano())
				srvFail, err := util.InitServiceContainer(nameFail, "", fp)
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvFail.Terminate(context.Background()) })

				nameGood := fmt.Sprintf("blockscan-service-good-pre-publish-%d", time.Now().UnixNano())
				srvGood, err := util.InitServiceContainer(nameGood, "", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvGood.Terminate(context.Background()) })

				require.NoError(t, util.MineAnvilBlocks(context.Background(), 1))
			},
			check: func(t *testing.T) { assertKafkaAndMarker(t, rc) },
		},
		{
			name: "successfully scan log and publish where one replica fails before ack the stream",
			setup: func(t *testing.T) {
				fp := FPEnvBeforeAck

				nameFail := fmt.Sprintf("blockscan-service-fp-pre-ack-%d", time.Now().UnixNano())
				srvFail, err := util.InitServiceContainer(nameFail, "", fp)
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvFail.Terminate(context.Background()) })

				nameGood := fmt.Sprintf("blockscan-service-good-pre-ack-%d", time.Now().UnixNano())
				srvGood, err := util.InitServiceContainer(nameGood, "", "")
				require.NoError(t, err)
				t.Cleanup(func() { _ = srvGood.Terminate(context.Background()) })

				require.NoError(t, util.MineAnvilBlocks(context.Background(), 1))
			},
			check: func(t *testing.T) { assertKafkaAndMarker(t, rc) },
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			anvil, err := util.InitGanacheContainer()
			require.NoError(t, err)
			t.Cleanup(func() { _ = anvil.Terminate(context.Background()) })

			kctr, err := util.InitKafkaContainer()
			require.NoError(t, err)
			t.Cleanup(func() { _ = kctr.Terminate(context.Background()) })

			rctr, err := util.InitRedisContainer()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rctr.Terminate(context.Background()) })

			rc, err = util.NewRedisClient()
			require.NoError(t, err)
			t.Cleanup(func() { _ = rc.Close() })
			require.NoError(t, util.FlushDB(rc, context.Background()))
			tt.setup(t)
			tt.check(t)
		})
	}
}

func assertKafkaAndMarker(t *testing.T, rc *redis.Client) {
	key := viper.GetString("redis.streams.key")
	topic := viper.GetString("kafka.topic")
	brokers := viper.GetStringSlice("kafka.brokers")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	rec, err := util.ConsumeOneKafkaRecord(ctx, brokers, topic, 40*time.Second)
	require.NoError(t, err)
	require.NotNil(t, rec)

	deadline := time.Now().Add(30 * time.Second)
	pi := viper.GetInt("test.poll_interval_ms")
	if pi <= 0 {
		pi = 300
	}
	pollInterval := time.Duration(pi) * time.Millisecond
	var hash string
	for time.Now().Before(deadline) {
		es, err := rc.XRange(context.Background(), key, "-", "+").Result()
		if err == nil && len(es) > 0 {
			if v, ok := es[0].Values["hash"].(string); ok {
				hash = v
			}
			break
		}
		time.Sleep(pollInterval)
	}
	require.NotEmpty(t, hash)
	tag := util.ClusterHashTagForKey(key)
	prefix := viper.GetString("redis.lock.dedup_publish_block_prefix")
	marker := fmt.Sprintf("{%s}:%s:%s", tag, prefix, hash)

	markDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(markDeadline) {
		if rc.Exists(context.Background(), marker).Val() == 1 {
			break
		}
		time.Sleep(pollInterval)
	}
	require.Equal(t, int64(1), rc.Exists(context.Background(), marker).Val())

	group := viper.GetString("redis.streams.consumer_group")
	pendDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(pendDeadline) {
		p, err := rc.XPending(context.Background(), key, group).Result()
		if err == nil && p.Count == 0 {
			break
		}
		time.Sleep(pollInterval)
	}
	p, err := rc.XPending(context.Background(), key, group).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), p.Count)
	require.Equal(t, int64(1), rc.XLen(context.Background(), key).Val())
}
