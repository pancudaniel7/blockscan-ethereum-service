//go:build integration

package containers

import (
    "context"
    "fmt"

    "github.com/testcontainers/testcontainers-go/modules/kafka"

    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/twmb/franz-go/pkg/kerr"
    "github.com/twmb/franz-go/pkg/kmsg"
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

    cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
    if err != nil {
        return fmt.Errorf("init kafka client: %w", err)
    }
    defer cl.Close()

    req := kmsg.NewCreateTopicsRequest()
    req.TimeoutMillis = 15000
    t := kmsg.NewCreateTopicsRequestTopic()
    t.Topic = name
    if partitions == 0 {
        partitions = -1 // broker default if supported
    }
    if replication == 0 {
        replication = -1 // broker default if supported
    }
    t.NumPartitions = partitions
    t.ReplicationFactor = replication
    for k, v := range configs {
        c := kmsg.NewCreateTopicsRequestTopicConfig()
        c.Name = k
        c.Value = v
        t.Configs = append(t.Configs, c)
    }
    req.Topics = append(req.Topics, t)

    respAny, err := cl.Request(ctx, &req)
    if err != nil {
        return fmt.Errorf("create topics request failed: %w", err)
    }
    resp := respAny.(*kmsg.CreateTopicsResponse)
    for _, tr := range resp.Topics {
        if tr.Topic != name {
            continue
        }
        if tr.ErrorCode == 0 {
            return nil
        }
        if err := kerr.ErrorForCode(tr.ErrorCode); err != nil {
            if err == kerr.TopicAlreadyExists {
                return nil
            }
            msg := ""
            if tr.ErrorMessage != nil {
                msg = *tr.ErrorMessage
            }
            return fmt.Errorf("create topic error: %v %s", err, msg)
        }
    }
    return fmt.Errorf("topic %q missing in create topics response", name)
}

// Terminate stops and removes the underlying container.
func (k *KafkaContainer) Terminate(ctx context.Context) error {
	if k == nil || k.container == nil {
		return nil
	}
	return k.container.Terminate(ctx)
}
