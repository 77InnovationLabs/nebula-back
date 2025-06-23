package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

func startKafka(kafka_brokers string) error {
	// Garantir que os tópicos existem
	err := ensureKafkaTopics(kafka_brokers)
	if err != nil {
		log.Fatalf("Erro ao garantir a existência dos tópicos Kafka: %v", err)
		return err
	}

	return nil
}
func ensureKafkaTopics(kafka_brokers string) error {
	// Configurar os tópicos Kafka
	brokers := strings.Split(kafka_brokers, ",")
	topics := []string{
		"pessoa.saved",
		"pessoa.deleted",
	}

	for _, topic := range topics {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return err
		}
		defer conn.Close()

		controller, err := conn.Controller()
		if err != nil {
			return err
		}
		address := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
		conn, err = kafka.Dial("tcp", address)
		if err != nil {
			return err
		}
		defer conn.Close()

		topicConfigs := kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}

		err = conn.CreateTopics(topicConfigs)
		if err != nil {
			if strings.Contains(err.Error(), "Topic with this name already exists") {
				log.Printf("Tópico %s já existe\n", topic)
			} else {
				return err
			}
		} else {
			log.Printf("Tópico %s criado com sucesso\n", topic)
		}
	}
	return nil
}
