// (build tags removed to run in default test builds)

package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	util "github.com/pancudaniel7/blockscan-ethereum-service/test/util"
	"github.com/spf13/viper"
)

var (
	kafkaCtr *util.KafkaContainer
	redisCtr *util.RedisContainer
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
	ctr, err := util.StartKafka(ctx, "")
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
	// Mirror deployments/kafka/topics.sh by defaulting to compaction.
	cleanup := "compact"
	configs := map[string]*string{
		"cleanup.policy": &cleanup,
	}
	if err := ctr.EnsureTopic(ctx, topic, 1, 1, configs); err != nil {
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
	rc, err := util.StartRedis(ctx)
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

    // Resolve function file across common working dirs (package test vs repo root).
    candidates := []string{
        "deployments/redis/functions/add_block.lua",
        "../deployments/redis/functions/add_block.lua",
        "../../deployments/redis/functions/add_block.lua",
        "./deployments/redis/functions/add_block.lua",
    }
    var found string
    for _, p := range candidates {
        if _, err := os.Stat(p); err == nil {
            found = p
            break
        }
    }
    if found == "" {
        return fmt.Errorf("redis function file not found in candidates: %v", candidates)
    }
    if err := rc.LoadFunctionFromFile(ctx, found); err != nil {
        return fmt.Errorf("load redis function from %s: %w", found, err)
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
	// Load test config so viper keys exist for provisioning (topic name, etc.).
	_ = util.InitConfig()
	// Best-effort bootstrap of external deps for integration runs.
	_ = InitContainers()
	code := m.Run()
	CleanupContainers()
	os.Exit(code)
}
