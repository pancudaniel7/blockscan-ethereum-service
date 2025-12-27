package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	dockercfg "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/spf13/viper"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ServiceContainer struct {
	container tc.Container
}

func dockerReachableHost(addr string) string {
	h := addr
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		h = addr[:i]
	}
	switch h {
	case "127.0.0.1", "localhost", "::1":
		return "host.docker.internal"
	}
	return h
}

func rewriteBrokersForDocker(addrs []string) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		h, p := a, ""
		if i := strings.LastIndexByte(a, ':'); i >= 0 {
			h = a[:i]
			if i+1 < len(a) {
				p = a[i+1:]
			}
		}
		out = append(out, fmt.Sprintf("%s:%s", dockerReachableHost(h), p))
	}
	return out
}

// BuildAndStartService builds (or reuses a tagged image) and starts the app container.
// If service.image_tag is set in configs/test.yml, that image is used directly.
// - If the tag exists locally, it is reused.
// - If it is missing, the image is built (with optional FAILPOINT) and tagged.
// If no tag is configured, the image is built from build/Dockerfile.test without tagging.
func BuildAndStartService(ctx context.Context, serviceName, tag, failpoint string) (*ServiceContainer, error) {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		tag = strings.TrimSpace(viper.GetString("service.image_tag"))
	}
	svcPort := viper.GetInt("service.port")
	if svcPort <= 0 {
		svcPort = 8081
	}
	portProto := fmt.Sprintf("%d/tcp", svcPort)

	req := tc.ContainerRequest{
		Name:         serviceName,
		ExposedPorts: []string{portProto},
		Env: map[string]string{
			"GO_ENV":                      "test",
			"CONFIG_NAME":                 "test",
			"BSCAN_REDIS_HOST":            dockerReachableHost(viper.GetString("redis.host")),
			"BSCAN_REDIS_PORT":            viper.GetString("redis.port"),
			"BSCAN_SCANNER_WEBSOCKET_URL": viper.GetString("scanner.websocket_url"),
			"BSCAN_KAFKA_BROKERS":         strings.Join(rewriteBrokersForDocker(viper.GetStringSlice("kafka.brokers")), ","),
			"BSCAN_KAFKA_TOPIC":           viper.GetString("kafka.topic"),
			"BSCAN_SERVICE_NAME":          viper.GetString("service.name"),
		},
		HostConfigModifier: func(hc *dockercfg.HostConfig) {
			hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
		},
	}

	if tag == "" {
		return nil, fmt.Errorf("service image tag is empty")
	}
	req.Image = tag

	req.WaitingFor = wait.ForListeningPort(nat.Port(portProto)).WithStartupTimeout(90 * time.Second)
	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, fmt.Errorf("start service container: %w", err)
	}
	return &ServiceContainer{container: ctr}, nil
}

func (a *ServiceContainer) URL(ctx context.Context) (string, error) {
	addr, err := a.Socket(ctx)
	if err != nil {
		return "", err
	}
	return "http://" + addr, nil
}

func (a *ServiceContainer) Socket(ctx context.Context) (string, error) {
	host, err := a.container.Host(ctx)
	if err != nil {
		return "", err
	}
	svcPort := viper.GetInt("service.port")
	if svcPort <= 0 {
		svcPort = 8081
	}
	portProto := fmt.Sprintf("%d/tcp", svcPort)
	port, err := a.container.MappedPort(ctx, nat.Port(portProto))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}

func (a *ServiceContainer) Terminate(ctx context.Context) error {
	if a == nil || a.container == nil {
		return nil
	}
	return a.container.Terminate(ctx)
}
