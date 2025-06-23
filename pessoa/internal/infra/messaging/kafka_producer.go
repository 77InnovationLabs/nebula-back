package messaging

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	Writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	if topic == "" {
		log.Fatal("Kafka topic must be specified")
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})

	return &KafkaProducer{
		Writer: writer,
	}
}

func (p *KafkaProducer) PublishMessage(ctx context.Context, key, value string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	err := p.Writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Erro ao publicar mensagem no Kafka: %v", err)
		return err
	}
	log.Printf("Mensagem publicada no Kafka: %s", value)
	return nil
}

func (p *KafkaProducer) Close() error {
	return p.Writer.Close()
}

// Verifica se KafkaProducer implementa KafkaProducerInterface
var _ KafkaProducerInterface = &KafkaProducer{}
