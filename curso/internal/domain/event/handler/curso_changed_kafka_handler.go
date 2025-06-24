package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/dto"
	msg_kafka "github.com/77InnovationLabs/nebula-back/curso/internal/infra/messaging/kafka"
	event_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
)

type CursoChangedKafkaHandler struct {
	KafkaProducer *msg_kafka.KafkaProducer
}

func NewCursoChangedKafkaHandler(kafkaProducer *msg_kafka.KafkaProducer) *CursoChangedKafkaHandler {
	return &CursoChangedKafkaHandler{
		KafkaProducer: kafkaProducer,
	}
}

func (h *CursoChangedKafkaHandler) Handle(event event_pkg.EventInterface, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Curso Changed sent to kafka: %v", event.GetPayload())

	jsonOutput, err := json.Marshal(event.GetPayload())
	if err != nil {
		fmt.Printf("Erro ao converter payload para JSON: %v", err)
		return
	}

	id := event.GetPayload().(dto.CursoOutputDTO).ID.String()

	err = h.KafkaProducer.PublishMessage(context.Background(), id, string(jsonOutput))
	if err != nil {
		fmt.Printf("Erro ao publicar mensagem no Kafka: %v", err)
		return
	}
	fmt.Printf("Mensagem publicada no Kafka: %s", string(jsonOutput))
}
