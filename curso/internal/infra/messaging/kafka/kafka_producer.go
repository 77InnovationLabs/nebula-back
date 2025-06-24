package kafka

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
	Topic    string
}

func NewKafkaProducer(brokers []string, topic, username, password string) *KafkaProducer {
	if topic == "" {
		log.Fatal("❌ Erro: Tópico Kafka deve ser especificado")
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	// SASL/SSL para Confluent Cloud
	if os.Getenv("ENV") == "LOCAL" {
		config.Net.SASL.Enable = false
		config.Net.TLS.Enable = false
	} else {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{}
	}

	// Timeouts de rede recomendados
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	config.Version = sarama.V2_5_0_0 // compatível com brokers Confluent Cloud

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("❌ Erro ao criar producer Sarama: %v", err)
	}

	log.Printf("✅ Kafka Producer pronto | Brokers: %v | Tópico: %s", brokers, topic)

	return &KafkaProducer{
		Producer: producer,
		Topic:    topic,
	}
}

func (p *KafkaProducer) PublishMessage(ctx context.Context, key, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := p.Producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Erro ao publicar mensagem Kafka: %v", err)
		return err
	}

	log.Printf("📩 Mensagem publicada! Tópico: %s | Partição: %d | Offset: %d | Valor: %s",
		p.Topic, partition, offset, value)

	return nil
}

func (p *KafkaProducer) Close() error {
	return p.Producer.Close()
}

// Verifica se KafkaProducer implementa KafkaProducerInterface
var _ KafkaProducerInterface = &KafkaProducer{}
