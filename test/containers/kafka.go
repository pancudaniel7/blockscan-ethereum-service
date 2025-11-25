package containers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaContainer wraps a Kafka testcontainer and admin helpers.
type KafkaContainer struct {
	container *kafka.KafkaContainer
}

// StartKafka launches a single-broker Kafka (KRaft) using the Confluent image.
// If image is empty, defaults to "confluentinc/confluent-local:7.5.0".
func StartKafka(ctx context.Context, image string) (*KafkaContainer, error) {
	img := image
	if img == "" {
		img = "confluentinc/confluent-local:7.5.0"
	}
	ctr, err := kafka.Run(ctx, img)
	if err != nil {
		return nil, fmt.Errorf("start kafka container: %w", err)
	}
	return &KafkaContainer{container: ctr}, nil
}

// Brokers returns the externally reachable bootstrap servers (host:port).
func (k *KafkaContainer) Brokers(ctx context.Context) ([]string, error) {
	return k.container.Brokers(ctx)
}

// EnsureTopic creates a topic if it does not already exist.
// partitions=-1 and replication=-1 let the broker defaults apply on 2.4+.
func (k *KafkaContainer) EnsureTopic(ctx context.Context, name string, partitions int32, replication int16, configs map[string]*string) error {
	brokers, err := k.Brokers(ctx)
	if err != nil {
		return err
	}

	adm, err := kadm.NewOptClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return fmt.Errorf("init kadm client: %w", err)
	}
	defer adm.Close()

	resp, err := adm.CreateTopic(ctx, partitions, replication, configs, name)
	if err != nil {
		return fmt.Errorf("create topic request failed: %w", err)
	}
	if resp.Err != nil {
		// Not an error if it already exists
		if kerrStr := resp.Err.Error(); kerrStr != "topic already exists" {
			return fmt.Errorf("create topic response error: %v (message=%s)", resp.Err, resp.ErrMessage)
		}
	}
	return nil
}

// Terminate stops and removes the underlying container.
func (k *KafkaContainer) Terminate(ctx context.Context) error {
	if k == nil || k.container == nil {
		return nil
	}
	return k.container.Terminate(ctx)
}
