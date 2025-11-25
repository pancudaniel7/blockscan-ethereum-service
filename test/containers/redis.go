package containers

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/docker/go-connections/nat"
	redislib "github.com/redis/go-redis/v9"
	redismodule "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisContainer wraps a testcontainers Redis container and exposes handy helpers.
type RedisContainer struct {
	container *redismodule.RedisContainer
}

// StartRedis launches a Redis container. Optionally specify a custom image (e.g. "redis:8.2.3").
func StartRedis(ctx context.Context, image ...string) (*RedisContainer, error) {
	img := "redis:7"
	if len(image) > 0 && strings.TrimSpace(image[0]) != "" {
		img = image[0]
	}

	ctr, err := redismodule.Run(ctx, img)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
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

// Client returns a go-redis client connected to the container.
func (r *RedisContainer) Client(ctx context.Context) (*redislib.Client, error) {
	addr, err := r.Address(ctx)
	if err != nil {
		return nil, err
	}
	return redislib.NewClient(&redislib.Options{Addr: addr}), nil
}

// LoadFunctionFromFile loads a Redis functions library into the container using FUNCTION LOAD REPLACE.
// The file content must include a proper shebang (e.g., "#!lua name=blockchain").
func (r *RedisContainer) LoadFunctionFromFile(ctx context.Context, path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read function file: %w", err)
	}

	// Normalize newlines (mirror deployments/redis/load_functions.sh behaviour)
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

// FlushDB flushes the default database on the container.
func (r *RedisContainer) FlushDB(ctx context.Context) error {
	cli, err := r.Client(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()
	return cli.FlushDB(ctx).Err()
}

// Terminate stops and removes the underlying container.
func (r *RedisContainer) Terminate(ctx context.Context) error {
	if r == nil || r.container == nil {
		return nil
	}
	return r.container.Terminate(ctx)
}
