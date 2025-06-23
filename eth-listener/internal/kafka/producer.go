package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	kafkago "github.com/segmentio/kafka-go"
)

// NewKafkaWriter cria um produtor Kafka e valida o tópico
func NewKafkaWriter(broker, topic string) (*kafkago.Writer, error) {
	if topic == "" {
		return nil, errors.New("❌ Erro: O tópico Kafka não pode estar vazio")
	}

	log.Printf("🔗 Conectando ao Kafka Broker: %s | Tópico: %s", broker, topic)

	return &kafkago.Writer{
		Addr:     kafkago.TCP(broker),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}, nil
}

// EnsureTopics verifica se os tópicos existem e os cria se necessário
func EnsureTopics(broker string, topics []string) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("❌ Erro ao conectar ao Kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("❌ Erro ao obter controlador do Kafka: %v", err)
	}

	controllerConn, err := kafkago.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("❌ Erro ao conectar ao controlador Kafka: %v", err)
	}
	defer controllerConn.Close()

	existingTopics, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("❌ Erro ao listar tópicos: %v", err)
	}

	existingTopicSet := make(map[string]struct{})
	for _, partition := range existingTopics {
		existingTopicSet[partition.Topic] = struct{}{}
	}

	for _, topic := range topics {
		if _, exists := existingTopicSet[topic]; !exists {
			log.Printf("⚠️ Tópico '%s' não encontrado. Criando...", topic)

			err := controllerConn.CreateTopics(kafkago.TopicConfig{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			})

			if err != nil {
				return fmt.Errorf("❌ Erro ao criar tópico '%s': %v", topic, err)
			}

			log.Printf("✅ Tópico '%s' criado com sucesso!", topic)
		} else {
			log.Printf("🔍 Tópico '%s' já existe.", topic)
		}
	}

	return nil
}

// SendEvent envia um evento para o Kafka, garantindo que o tópico esteja correto
func SendEvent(writer *kafkago.Writer, key, message string) error {
	if writer == nil {
		return errors.New("❌ Erro: Writer Kafka não foi inicializado")
	}

	err := writer.WriteMessages(context.Background(), kafkago.Message{
		Key:   []byte(key),
		Value: []byte(message),
	})

	if err != nil {
		log.Printf("❌ Erro ao enviar mensagem para Kafka: %v", err)
		return err
	}

	log.Printf("📩 Evento enviado para o Kafka com sucesso! Mensagem: %s", message)
	return nil
}
