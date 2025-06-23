package main

import (
	"fmt"
	"log"

	"eth-listener/config"
	"eth-listener/internal/ethereum"
	myethereum "eth-listener/internal/ethereum"
	mykafka "eth-listener/internal/kafka"

	"github.com/ethereum/go-ethereum/common"
)

func main() {
	fmt.Println("🚀 Iniciando serviço de escuta de eventos Ethereum...")
	fmt.Println("🔧 Carregando configurações do ambiente...")

	// Carregar configurações do ambiente (Docker Compose)
	cfg := config.LoadConfig()

	// Conectar ao nó Geth
	client := myethereum.NewEthereumClient(cfg.EthereumWSURL)
	contractAddress := common.HexToAddress(cfg.ContractAddress)

	// Obter a ABI do contrato
	contractABI, err := myethereum.GetContractABI(contractAddress.Hex())
	if err != nil {
		log.Fatalf("❌ Erro ao obter ABI do contrato: %v", err)
	}

	// Criar tópicos Kafka necessários
	err = mykafka.EnsureTopics(cfg.KafkaBroker, []string{cfg.KafkaTopic})
	if err != nil {
		log.Fatalf("❌ Erro ao criar tópicos Kafka: %v", err)
	}

	// Configurar Kafka
	writer, err := mykafka.NewKafkaWriter(cfg.KafkaBroker, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("❌ Erro ao inicializar Kafka Writer: %v", err)
	}

	// Buscar eventos passados antes de iniciar a escuta em tempo real
	lastProcessedBlock := ethereum.BuscarEventosPassados(client, contractAddress, contractABI, writer)

	// Iniciar escuta de eventos em tempo real
	ethereum.ListenEvents(client, contractAddress, contractABI, lastProcessedBlock, writer)
}
