package pkg

import (
    "context"
    "errors"
    "fmt"
    "net"
    "strconv"
    "strings"

	dockercfg "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaContainer wraps a Kafka testcontainer and admin helpers.
type KafkaContainer struct {
	container tc.Container
}

// StartKafka launches a single-broker Kafka (KRaft) using the Apache Kafka image.
// If the image is empty, defaults to "apache/kafka:3.8.0" and binds host 9092.
func StartKafka(ctx context.Context, image string) (*KafkaContainer, error) {
	img := image
	if img == "" {
		if v := strings.TrimSpace(viper.GetString("kafka.image")); v != "" {
			img = v
		} else {
			img = "apache/kafka:3.8.0"
		}
	}

    clientHostPort := 9092
    if bs := viper.GetStringSlice("kafka.brokers"); len(bs) > 0 {
        if i := strings.LastIndexByte(bs[0], ':'); i > 0 {
            if v, err := strconv.Atoi(bs[0][i+1:]); err == nil && v > 0 && v <= 65535 {
                clientHostPort = v
            }
        }
    }
    clientHostPort = chooseFreePort(clientHostPort)
    viper.Set("kafka.host_port", clientHostPort)
    // Container listens on 9092 for PLAINTEXT_HOST; bind that to a free host port.
    containerPort := nat.Port("9092/tcp")
	env := map[string]string{
		"KAFKA_NODE_ID":                                          "1",
		"KAFKA_PROCESS_ROLES":                                    "broker,controller",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":                   "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
		"KAFKA_LISTENERS":                                        "CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092",
		"KAFKA_ADVERTISED_LISTENERS":                             fmt.Sprintf("PLAINTEXT_HOST://host.docker.internal:%d,PLAINTEXT://localhost:19092", clientHostPort),
		"KAFKA_CONTROLLER_LISTENER_NAMES":                        "CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":                         "1@localhost:29093",
		"KAFKA_INTER_BROKER_LISTENER_NAME":                       "PLAINTEXT",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":                 "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":                    "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":         "1",
		"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR": "1",
		"KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR":            "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":                 "0",
		"KAFKA_LOG_DIRS":                                         "/tmp/kraft-combined-logs",
		"CLUSTER_ID": func() string {
			c := strings.TrimSpace(viper.GetString("kafka.cluster_id"))
			if c == "" {
				return "4L6g3nShT-eMCtK--X86sw"
			}
			return c
		}(),
	}
	if overrides := viper.GetStringMapString("kafka.env"); len(overrides) > 0 {
		for k, v := range overrides {
			if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
				continue
			}
			env[k] = v
		}
	}
    req := tc.ContainerRequest{
        Image:        img,
        ExposedPorts: []string{string(containerPort)},
        Env:          env,
        WaitingFor:   wait.ForListeningPort(containerPort),
    }
	req.HostConfigModifier = func(hc *dockercfg.HostConfig) {
        if hc.PortBindings == nil {
            hc.PortBindings = nat.PortMap{}
        }
        hc.PortBindings[containerPort] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: strconv.Itoa(clientHostPort)}}
        hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
    }

    ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, fmt.Errorf("start kafka dep: %w", err)
	}
	return &KafkaContainer{container: ctr}, nil
}

// chooseFreePort returns the preferred port if available on the host, otherwise
// it asks the OS for an ephemeral port and returns that value.
func chooseFreePort(preferred int) int {
    l, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        return preferred
    }
    defer l.Close()
    if addr, ok := l.Addr().(*net.TCPAddr); ok {
        return addr.Port
    }
    return preferred
}

func (k *KafkaContainer) Brokers(ctx context.Context) ([]string, error) {
    host, err := k.container.Host(ctx)
    if err != nil {
        return nil, err
    }
    // Resolve mapped host port for container's PLAINTEXT_HOST 9092
    port, err := k.container.MappedPort(ctx, nat.Port("9092/tcp"))
    if err != nil {
        return nil, err
    }
    return []string{fmt.Sprintf("%s:%s", host, port.Port())}, nil
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
		partitions = -1
	}
	if replication == 0 {
		replication = -1
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
			if errors.Is(err, kerr.TopicAlreadyExists) {
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

// Terminate stops and removes the underlying dep.
func (k *KafkaContainer) Terminate(ctx context.Context) error {
	if k == nil || k.container == nil {
		return nil
	}
	return k.container.Terminate(ctx)
}
