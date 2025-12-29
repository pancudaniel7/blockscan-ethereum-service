package pkg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	dockercfg "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	redislib "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RedisContainer wraps a testcontainers Redis dep and exposes handy helpers.
type RedisContainer struct {
	container tc.Container
}

// StartRedis launches a Redis dep. Optionally specify a custom image (e.g. "redis:8.2.3").
func StartRedis(ctx context.Context, image ...string) (*RedisContainer, error) {
	return StartRedisWithPort(ctx, 0, image...)
}

// StartRedisWithPort starts a Redis container and binds its 6379/tcp to the given hostPort when >0.
func StartRedisWithPort(ctx context.Context, hostPort int, image ...string) (*RedisContainer, error) {
	img := "redis:8.4.0"
	if len(image) > 0 && strings.TrimSpace(image[0]) != "" {
		img = image[0]
	} else if v := strings.TrimSpace(viper.GetString("redis.image")); v != "" {
		img = v
	}

	port := nat.Port("6379/tcp")
	req := tc.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{string(port)},
		WaitingFor:   wait.ForListeningPort(port),
	}
	if hostPort > 0 {
		req.HostConfigModifier = func(hc *dockercfg.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = nat.PortMap{}
			}
			hc.PortBindings[port] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: strconv.Itoa(hostPort)}}
		}
	}

	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, fmt.Errorf("start redis dep: %w", err)
	}
	return &RedisContainer{container: ctr}, nil
}

// Address returns "host:port" for connecting with non-TLS clients.
func (r *RedisContainer) Address(ctx context.Context) (string, error) {
	h, err := r.container.Host(ctx)
	if err != nil {
		return "", err
	}
	p, err := r.container.MappedPort(ctx, nat.Port("6379/tcp"))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", h, p.Port()), nil
}

// Client returns a go-redis client connected to the dep.
func (r *RedisContainer) Client(ctx context.Context) (*redislib.Client, error) {
	addr, err := r.Address(ctx)
	if err != nil {
		return nil, err
	}
	return redislib.NewClient(&redislib.Options{Addr: addr}), nil
}

// LoadFunctionFromFile loads a Redis functions library into the dep using FUNCTION LOAD REPLACE.
// The file content must include a proper shebang (e.g., "#!lua name=blockchain").
func (r *RedisContainer) LoadFunctionFromFile(ctx context.Context, path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read function file: %w", err)
	}

	source := strings.ReplaceAll(string(content), "\r", "")

	cli, err := r.Client(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	if _, err := cli.Do(ctx, "FUNCTION", "LOAD", "REPLACE", source).Result(); err != nil {
		return fmt.Errorf("load redis function: %w", err)
	}
	return nil
}

// FlushDB flushes the default database on the dep.
func (r *RedisContainer) FlushDB(ctx context.Context) error {
	cli, err := r.Client(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	return cli.FlushDB(ctx).Err()
}

// Terminate stops and removes the underlying dep.
func (r *RedisContainer) Terminate(ctx context.Context) error {
	if r == nil || r.container == nil {
		return nil
	}
	return r.container.Terminate(ctx)
}

// StartRedisContainer starts a Redis testcontainer, points Viper's redis.{host,port}
// to it, and loads the add_block Redis function. The caller owns the returned
// container and should Terminate it when done (e.g., via t.Cleanup).
func StartRedisContainer(ctx context.Context) (*RedisContainer, error) {
	rc, err := StartRedisWithPort(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}

	addr, err := rc.Address(ctx)
	if err != nil {
		_ = rc.Terminate(ctx)
		return nil, fmt.Errorf("redis address: %w", err)
	}
	host, port := addr, ""
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		host = addr[:i]
		if i+1 < len(addr) {
			port = addr[i+1:]
		}
	}
	if host == "" || port == "" {
		_ = rc.Terminate(ctx)
		return nil, fmt.Errorf("invalid redis address: %q", addr)
	}
	viper.Set("redis.host", host)
	viper.Set("redis.port", port)

	root, ferr := findRepoRoot()
	functionPath := ""
	if ferr == nil {
		p := filepath.Join(root, "deployments", "redis", "functions", "add_block.lua")
		if _, err := os.Stat(p); err == nil {
			functionPath = p
		}
	}
	if functionPath == "" {
		fallback := "deployments/redis/functions/add_block.lua"
		if _, err := os.Stat(fallback); err == nil {
			functionPath = fallback
		}
	}
	if functionPath == "" {
		_ = rc.Terminate(ctx)
		return nil, fmt.Errorf("redis function file not found")
	}
	if err := rc.LoadFunctionFromFile(ctx, functionPath); err != nil {
		_ = rc.Terminate(ctx)
		return nil, fmt.Errorf("load redis function from %s: %w", functionPath, err)
	}

	return rc, nil
}
