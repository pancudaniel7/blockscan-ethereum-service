package infra

import (
    "context"
    "net/http"
    _ "net/http/pprof"
    "runtime"
    "sync"
    "time"

    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
    "github.com/spf13/viper"
)

// StartPprof starts a dedicated net/http pprof server when enabled via config.
// Returns a stop function that can gracefully shutdown the server.
func StartPprof(logger applog.AppLogger, wg *sync.WaitGroup) func(context.Context) error {
    if !viper.GetBool("pprof.enabled") {
        return func(context.Context) error { return nil }
    }

    addr := viper.GetString("pprof.addr")
    if addr == "" {
        addr = "127.0.0.1:6060"
    }
    if n := viper.GetInt("pprof.block_profile_rate"); n > 0 {
        runtime.SetBlockProfileRate(n)
    }
    if n := viper.GetInt("pprof.mutex_profile_fraction"); n > 0 {
        runtime.SetMutexProfileFraction(n)
    }

    srv := &http.Server{Addr: addr}
    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            logger.Warn("pprof server error", "err", err)
        }
    }()

    return func(ctx context.Context) error {
        if ctx == nil {
            c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
            defer cancel()
            return srv.Shutdown(c)
        }
        return srv.Shutdown(ctx)
    }
}

