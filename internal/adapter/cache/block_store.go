package cache

import (
	"context"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis/v8"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

type Config struct {
	Host               string `validate:"required,hostname|ip"`
	Port               string `validate:"required,numeric"`
	Password           string
	DB                 int `validate:"gte=0"`
	UseTLS             bool
	PoolSize           int `validate:"gte=0"`
	MaxRetries         int `validate:"gte=0"`
	DialTimeoutSeconds int `validate:"gte=0"`
}

type BlockStore struct {
	rdb       *redis.Client
	logger    applog.AppLogger
	wg        *sync.WaitGroup
	cancel    context.CancelFunc
	validator *validator.Validate
}

func NewBlockStore(rdb *redis.Client, logger applog.AppLogger, wg *sync.WaitGroup, validator *validator.Validate) *BlockStore {
	return &BlockStore{
		rdb:       rdb,
		logger:    logger,
		wg:        wg,
		validator: validator,
	}
}
