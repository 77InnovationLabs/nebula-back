package main

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Estrutura do consumidor usando Sarama
type KafkaConsumer struct {
	Topic   string
	GroupID string
	Brokers []string
	Handler sarama.ConsumerGroupHandler
}

// Config global Sarama para reuso
func newSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0

	// SASL + TLS para Confluent Cloud
	if os.Getenv("ENV") == "LOCAL" {
		config.Net.SASL.Enable = false
		config.Consumer.Return.Errors = false
	} else {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = os.Getenv("KAFKA_KEY")
		config.Net.SASL.Password = os.Getenv("KAFKA_SECRET")
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.TLS.Enable = true
	}
	config.Consumer.Return.Errors = true
	return config
}

// StartConsumers inicia todos os ConsumerGroups
func startKafkaConsumers(consumers []*KafkaConsumer) {
	var wg sync.WaitGroup

	for _, consumer := range consumers {
		wg.Add(1)
		go func(c *KafkaConsumer) {
			defer wg.Done()

			config := newSaramaConfig()
			client, err := sarama.NewConsumerGroup(c.Brokers, c.GroupID, config)
			if err != nil {
				log.Fatalf("❌ Erro ao criar ConsumerGroup: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			for {
				err := client.Consume(ctx, []string{c.Topic}, c.Handler)
				if err != nil {
					log.Printf("❌ Erro no ConsumerGroup %s: %v", c.GroupID, err)
					time.Sleep(5 * time.Second)
				}
			}
		}(consumer)
	}

	wg.Wait()
}

// Exemplo para criar tópicos usando Sarama
func ensureKafkaTopics(brokers []string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0 // Compatível com Confluent Cloud

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer admin.Close()

	topics := []string{
		"curso.saved",
		"curso.deleted",
		"pessoa.saved",
		"pessoa.deleted",
		"aluno.saved",
		"aluno.deleted",
		"modulo.saved",
		"modulo.deleted",
	}

	for _, topic := range topics {
		err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
		if err != nil {
			// O Sarama retorna TopicError embutido no erro
			if e, ok := err.(*sarama.TopicError); ok {
				if e.Err == sarama.ErrTopicAlreadyExists {
					log.Printf("🔍 Tópico %s já existe.", topic)
					continue
				}
			}
			return err
		}
		log.Printf("✅ Tópico %s criado com sucesso!", topic)
	}

	return nil
}

// Handler base para ConsumerGroup
type SimpleHandler struct {
	handleFunc func(msg *sarama.ConsumerMessage) error
}

func (h *SimpleHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *SimpleHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *SimpleHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		err := h.handleFunc(msg)
		if err == nil {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

// Exemplo de inicialização do Kafka em main:
func startKafka(brokers string) error {
	broker_list := strings.Split(brokers, ",")
	err := ensureKafkaTopics(broker_list)
	if err != nil {
		log.Fatalf("❌ Erro ao criar/verificar tópicos: %v", err)
		return err
	}
	return nil
}
