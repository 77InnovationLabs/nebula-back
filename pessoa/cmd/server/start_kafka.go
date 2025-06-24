package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

// Inicializa Sarama e garante os tópicos necessários
func startKafka(brokers string) error {
	brokerList := strings.Split(brokers, ",")

	// Criar configuração Sarama
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0 // ou outra versão compatível com Confluent Cloud
	config.Producer.Return.Successes = true

	if os.Getenv("ENV") == "LOCAL" {
		config.Net.SASL.Enable = false
		config.Net.TLS.Enable = false
	} else {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = os.Getenv("KAFKA_KEY")
		config.Net.SASL.Password = os.Getenv("KAFKA_SECRET")
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.TLS.Enable = true
	}

	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		return fmt.Errorf("❌ Erro ao criar ClusterAdmin: %v", err)
	}
	defer admin.Close()

	if os.Getenv("ENV") == "LOCAL" {

		topics := []string{
			"pessoa.saved",
			"pessoa.deleted",
		}

		for _, topic := range topics {
			err := admin.CreateTopic(topic, &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 3,
			}, false)

			if err != nil {
				if err.(sarama.KError) == sarama.ErrTopicAlreadyExists {
					log.Printf("🔍 Tópico '%s' já existe.", topic)
				} else {
					return fmt.Errorf("❌ Erro ao criar tópico '%s': %v", topic, err)
				}
			} else {
				log.Printf("✅ Tópico '%s' criado com sucesso!", topic)
			}
		}
	}

	return nil
}
