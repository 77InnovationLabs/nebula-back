package config

import (
	"os"
)

// Config armazena as variáveis do serviço
type Config struct {
	EthereumRPCURL  string
	EthereumWSURL   string
	ContractAddress string
	KafkaBroker     string
	KafkaTopic      string
}

// LoadConfig carrega as configurações do ambiente
func LoadConfig() *Config {
	return &Config{
		EthereumRPCURL:  os.Getenv("ETHEREUM_RPC_URL"),
		EthereumWSURL:   os.Getenv("ETHEREUM_WS_URL"),
		ContractAddress: os.Getenv("CONTRACT_ADDRESS"),
		KafkaBroker:     os.Getenv("KAFKA_BROKER"),
		KafkaTopic:      os.Getenv("KAFKA_TOPIC"),
	}
}
