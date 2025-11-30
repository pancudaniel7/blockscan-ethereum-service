//go:build integration
// +build integration

package test

import (
    "context"
    "fmt"
    "strings"
    "os"

    "github.com/spf13/viper"
    testcontainers "github.com/pancudaniel7/blockscan-ethereum-service/test/containers"
)

var (
    kafkaCtr *testcontainers.KafkaContainer
    redisCtr *testcontainers.RedisContainer
)

func InitContainers() error {
    if err := InitKafkaContainer(); err != nil {
        return err
    }
    return InitRedisContainer()
}

func InitKafkaContainer() error {
    if kafkaCtr != nil {
        _ = kafkaCtr.Terminate(context.Background())
        kafkaCtr = nil
    }

    ctx := context.Background()
    ctr, err := testcontainers.StartKafka(ctx, "")
	if err != nil {
		return fmt.Errorf("start kafka container: %w", err)
	}
	kafkaCtr = ctr

	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		return fmt.Errorf("kafka brokers: %w", err)
	}
    viper.Set("kafka.brokers", brokers)

	topic := viper.GetString("kafka.topic")
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("kafka topic not configured")
	}
    if err := ctr.EnsureTopic(ctx, topic, 1, 1, nil); err != nil {
        return fmt.Errorf("ensure topic: %w", err)
    }
    return nil
}

func InitRedisContainer() error {
    if redisCtr != nil {
        _ = redisCtr.Terminate(context.Background())
        redisCtr = nil
    }

    ctx := context.Background()
    rc, err := testcontainers.StartRedis(ctx)
	if err != nil {
		return fmt.Errorf("start redis container: %w", err)
	}
	redisCtr = rc

	addr, err := rc.Address(ctx)
	if err != nil {
		return fmt.Errorf("redis address: %w", err)
	}
	host := addr
	port := ""
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		host = addr[:i]
		port = addr[i+1:]
	}
	if host == "" || port == "" {
		return fmt.Errorf("invalid redis address: %q", addr)
	}
	viper.Set("redis.host", host)
    viper.Set("redis.port", port)

	path := "deployments/redis/functions/add_block.lua"
    if err := rc.LoadFunctionFromFile(ctx, path); err != nil {
        return fmt.Errorf("load redis function from %s: %w", path, err)
    }
    return nil
}

// CleanupContainers terminates any running test containers.
func CleanupContainers() {
    ctx := context.Background()
    if kafkaCtr != nil {
        _ = kafkaCtr.Terminate(ctx)
        kafkaCtr = nil
    }
    if redisCtr != nil {
        _ = redisCtr.Terminate(ctx)
        redisCtr = nil
    }
}

// TestMain ensures containers are torn down when the test process exits.
func TestMain(m *testing.M) {
    code := m.Run()
    CleanupContainers()
    os.Exit(code)
}
