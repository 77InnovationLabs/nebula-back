package messaging

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
	Topic    string
}

// NewKafkaProducer cria um produtor específico para um tópico usando um SyncProducer global.
func NewKafkaProducer(producer sarama.SyncProducer, topic string) *KafkaProducer {
	if topic == "" {
		log.Fatal("❌ Erro: O tópico Kafka deve ser especificado")
	}

	return &KafkaProducer{
		Producer: producer,
		Topic:    topic,
	}
}

// PublishMessage publica uma mensagem usando o Sarama Producer.
func (p *KafkaProducer) PublishMessage(ctx context.Context, key, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: p.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := p.Producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Erro ao publicar mensagem no Kafka: %v", err)
		return err
	}

	log.Printf("📩 Mensagem publicada no Kafka! Tópico: %s | Partição: %d | Offset: %d | Valor: %s",
		p.Topic, partition, offset, value)

	return nil
}

// Verifica se KafkaProducer implementa KafkaProducerInterface
var _ KafkaProducerInterface = &KafkaProducer{}
