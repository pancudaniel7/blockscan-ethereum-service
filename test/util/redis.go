package util

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func NewRedisClient(host, port string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: host + ":" + port})
}

func NewRedisClientFromConfig() (*redis.Client, error) {

	return redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.host") + ":" + viper.GetString("redis.port"),
		Password: viper.GetString("redis.password"),
		DB:       viper.GetInt("redis.db"),
	}), nil
}

func FlushDB(rdb *redis.Client, ctx context.Context) error {
	return rdb.FlushDB(ctx).Err()
}

func CallAddBlockFunction(rdb *redis.Client, ctx context.Context, setKey, streamKey, ttl, id string, xaddArgs ...string) ([]interface{}, error) {
	cmd := make([]interface{}, 0, 7+len(xaddArgs))
	cmd = append(cmd, "FCALL", "add_block", 2, setKey, streamKey, ttl, id)
	for _, v := range xaddArgs {
		cmd = append(cmd, v)
	}
	res, err := rdb.Do(ctx, cmd...).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("FCALL result is not a slice, got %T", res)
	}
	return arr, nil
}

func GetValue(rdb *redis.Client, ctx context.Context, key string) (string, error) {
	return rdb.Get(ctx, key).Result()
}

func XAck(rdb *redis.Client, ctx context.Context, streamKey, consumerGroup string, messageIDs ...string) (int64, error) {
	return rdb.XAck(ctx, streamKey, consumerGroup, messageIDs...).Result()
}

func XDel(rdb *redis.Client, ctx context.Context, streamKey string, messageIDs ...string) (int64, error) {
	return rdb.XDel(ctx, streamKey, messageIDs...).Result()
}

func XTrim(rdb *redis.Client, ctx context.Context, streamKey string, maxLen int64) (int64, error) {
	return rdb.XTrimMaxLen(ctx, streamKey, maxLen).Result()
}

func XTrimApprox(rdb *redis.Client, ctx context.Context, streamKey string, maxLen int64) (int64, error) {
	return rdb.XTrimMaxLenApprox(ctx, streamKey, maxLen, 0).Result()
}

func CreateConsumerGroup(rdb *redis.Client, ctx context.Context, streamKey, groupName string) error {
	return rdb.XGroupCreateMkStream(ctx, streamKey, groupName, "0").Err()
}

func ReadFromGroup(rdb *redis.Client, ctx context.Context, streamKey, groupName, consumerName string, count int64) ([]redis.XStream, error) {
	return rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    count,
		Block:    0,
	}).Result()
}

func AckAndDeleteMessage(rdb *redis.Client, ctx context.Context, streamKey, consumerGroup, messageID string) error {
	if _, err := XAck(rdb, ctx, streamKey, consumerGroup, messageID); err != nil {
		return fmt.Errorf("failed to acknowledge message: %w", err)
	}

	if _, err := XDel(rdb, ctx, streamKey, messageID); err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	return nil
}
