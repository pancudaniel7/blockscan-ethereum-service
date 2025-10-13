package util

import (
	"fmt"
	"log"

	"github.com/spf13/viper"
)

func InitConfig() error {
	viper.SetConfigName("test")
	viper.SetConfigType("yml")
	viper.AddConfigPath("../configs")
	viper.AddConfigPath("../../configs")
	viper.AddConfigPath("./configs")

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	log.Printf("Config loaded from: %s", viper.ConfigFileUsed())
	return nil
}
