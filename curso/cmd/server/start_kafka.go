package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Estrutura que representa um consumidor Kafka
type KafkaConsumer struct {
	topic     string
	partition int
	brokers   []string
	handler   func(msg kafka.Message) error // Função handler para processar cada mensagem
}

// Função para inicializar o consumidor Kafka
func (kc *KafkaConsumer) StartConsuming(wg *sync.WaitGroup, broker string) {
	defer wg.Done() // Decrementa o contador do WaitGroup quando a função terminar

	// Inicializando o leitor (consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     kc.topic,
		Partition: kc.partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	// Consome mensagens indefinidamente
	for {
		// Timeout para leitura da mensagem
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Lendo a mensagem
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Erro ao ler mensagem do tópico '%s' no broker '%s': %v\n", kc.topic, broker, err)
			continue
		}

		// Executa o handler definido para o consumidor
		kc.handler(msg)
	}
}

// Função que inicializa múltiplos consumidores para cada broker
func startKafkaConsumers(consumers []*KafkaConsumer) {
	var wg sync.WaitGroup

	// Iniciar cada consumidor Kafka em uma goroutine para cada broker
	for _, consumer := range consumers {
		for _, broker := range consumer.brokers {
			wg.Add(1)
			go consumer.StartConsuming(&wg, broker)
		}
	}

	// Aguardar até que todos os consumidores terminem (não deve acontecer em um loop infinito)
	wg.Wait()
}

func startKafka(kafka_brokers string) error {
	// Garantir que os tópicos existem
	err := ensureKafkaTopics(kafka_brokers)
	if err != nil {
		log.Fatalf("Erro ao garantir a existência dos tópicos Kafka: %v", err)
		return err
	}

	//registra os consumidores
	// go startKafkaConsumer(kafka_brokers, "curso.saved")

	return nil
}

func ensureKafkaTopics(kafka_brokers string) error {
	// Configurar os tópicos Kafka
	brokers := strings.Split(kafka_brokers, ",")
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
