package pkg

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

func InitKafkaContainer() (*KafkaContainer, error) {
	ctx := context.Background()
	ctr, err := StartKafka(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("start kafka container: %w", err)
	}

	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka brokers: %w", err)
	}
	viper.Set("kafka.brokers", brokers)

	topic := viper.GetString("kafka.topic")
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("kafka topic not configured")
	}
	cleanup := "compact"
	configs := map[string]*string{
		"cleanup.policy": &cleanup,
	}
	if err := ctr.EnsureTopic(ctx, topic, 1, 1, configs); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}
	return ctr, nil
}

func InitRedisContainer() (*RedisContainer, error) {
	return StartRedisContainer(context.Background())
}

func InitServiceContainer(serviceName, imageTag, failpoint string) (*ServiceContainer, error) {
	ctx := context.Background()
	ctr, err := BuildAndStartService(ctx, serviceName, imageTag, failpoint)
	if err != nil {
		fmt.Println("failed to start service container:", err)
		return nil, err
	}
	return ctr, nil
}

func InitGanacheContainer() (*GanacheContainer, error) {
	ctx := context.Background()
	img := viper.GetString("ganache.image")
	port := viper.GetInt("ganache.port")
	chainId := viper.GetString("ganache.chain_id")
	extra := viper.GetStringSlice("ganache.extra_args")

	ctr, err := StartGanache(ctx, img, uint16(port), chainId, extra...)
	if err != nil {
		fmt.Println("failed to start ganache container:", err)
		return nil, err
	}

	addr, err := ctr.Address(ctx)
	if err != nil {
		return nil, err
	}
	p := ""
	if i := strings.LastIndexByte(addr, ':'); i >= 0 && i+1 < len(addr) {
		p = addr[i+1:]
	}
	if p == "" {
		return nil, fmt.Errorf("failed to parse ganache mapped port: %s", addr)
	}

	viper.Set("scanner.websocket_url", "ws://host.docker.internal:"+p)
	viper.Set("ganache.http_url", "http://"+addr)
	return ctr, nil
}
