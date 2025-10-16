package infra

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/viper"
)

const (
	configDir         = "configs"
	defaultConfigName = "local"
	envConfigNameKey  = "CONFIG_NAME"
	envPrefix         = "BSCAN"
)

var (
	loadConfigOnce sync.Once
	loadConfigErr  error
)

// LoadConfig makes Viper read the desired configuration file and wire env overrides.
func LoadConfig() error {
	loadConfigOnce.Do(func() {
		configName := strings.TrimSpace(os.Getenv(envConfigNameKey))
		if configName == "" {
			configName = defaultConfigName
		}

		viper.SetConfigName(configName)
		viper.SetConfigType("yml")
		registerConfigPaths()

		viper.SetEnvPrefix(envPrefix)
		viper.AutomaticEnv()
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

		if err := viper.ReadInConfig(); err != nil {
			loadConfigErr = fmt.Errorf("infra: failed to read config %q: %w", configName, err)
			return
		}
	})

	return loadConfigErr
}

func registerConfigPaths() {
	paths := []string{
		configDir,
		filepath.Join("..", configDir),
		filepath.Join("..", "..", configDir),
		filepath.Join(".", configDir),
		".",
	}
	for _, p := range paths {
		viper.AddConfigPath(p)
	}
}
