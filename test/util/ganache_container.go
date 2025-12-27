package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	dockercfg "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/spf13/viper"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type GanacheContainer struct {
	container tc.Container
}

func StartGanache(ctx context.Context, image string, hostPort uint16, chainID string, extra ...string) (*GanacheContainer, error) {
	img := strings.TrimSpace(image)
	if img == "" {
		img = "trufflesuite/ganache:v7.9.2"
	}

	port := nat.Port("8545/tcp")
	cmd := []string{"--host", "0.0.0.0", "--port", "8545"}
	if strings.TrimSpace(chainID) != "" {
		cmd = append(cmd, "--chain.chainId", strings.TrimSpace(chainID))
	}
	if len(extra) > 0 {
		cmd = append(cmd, extra...)
	}
	cmd = append(cmd, "--miner.blockTime", "0", "--miner.instamine", "strict")
	req := tc.ContainerRequest{
		Image:        img,
		ExposedPorts: []string{string(port)},
		Cmd:          cmd,
		WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(90 * time.Second),
	}

	if hostPort > 0 {
		req.HostConfigModifier = func(hc *dockercfg.HostConfig) {
			if hc.PortBindings == nil {
				hc.PortBindings = nat.PortMap{}
			}
			hc.PortBindings[port] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: strconv.Itoa(int(hostPort))}}
		}
	}

	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, fmt.Errorf("start ganache container: %w", err)
	}
	return &GanacheContainer{container: ctr}, nil
}

func (g *GanacheContainer) Address(ctx context.Context) (string, error) {
	if g == nil || g.container == nil {
		return "", fmt.Errorf("ganache container not initialized")
	}
	h, err := g.container.Host(ctx)
	if err != nil {
		return "", err
	}
	p, err := g.container.MappedPort(ctx, nat.Port("8545/tcp"))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", h, p.Port()), nil
}

func (g *GanacheContainer) Terminate(ctx context.Context) error {
	if g == nil || g.container == nil {
		return nil
	}
	return g.container.Terminate(ctx)
}

func MineAnvilBlocks(ctx context.Context, n int) error {
	url := strings.TrimSpace(viper.GetString("ganache.http_url"))
	if url == "" {
		return fmt.Errorf("ganache http url not set")
	}
	type req struct {
		JSONRPC string        `json:"jsonrpc"`
		Method  string        `json:"method"`
		Params  []interface{} `json:"params"`
		ID      int           `json:"id"`
	}
	enc := func(i int) ([]byte, error) {
		r := req{JSONRPC: "2.0", Method: "evm_mine", Params: []interface{}{}, ID: i}
		return json.Marshal(r)
	}
	for i := 0; i < n; i++ {
		body, err := enc(i + 1)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("evm_mine http status %d", resp.StatusCode)
		}
	}
	return nil
}
