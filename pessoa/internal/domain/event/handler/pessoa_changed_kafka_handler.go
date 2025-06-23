package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/dto"
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/infra/messaging"
	event_pkg "github.com/ggialluisi/fdqf-ms/pessoa/pkg/event_dispatcher"
)

type PessoaChangedKafkaHandler struct {
	KafkaProducer *messaging.KafkaProducer
}

func NewPessoaChangedKafkaHandler(kafkaProducer *messaging.KafkaProducer) *PessoaChangedKafkaHandler {
	return &PessoaChangedKafkaHandler{
		KafkaProducer: kafkaProducer,
	}
}

func (h *PessoaChangedKafkaHandler) Handle(event event_pkg.EventInterface, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Pessoa Changed sent to kafka: %v", event.GetPayload())

	jsonOutput, err := json.Marshal(event.GetPayload())
	if err != nil {
		fmt.Printf("Erro ao converter payload para JSON: %v", err)
		return
	}

	id := event.GetPayload().(dto.PessoaOutputDTO).ID.String()

	err = h.KafkaProducer.PublishMessage(context.Background(), id, string(jsonOutput))
	if err != nil {
		fmt.Printf("Erro ao publicar mensagem no Kafka: %v", err)
		return
	}
	fmt.Printf("Mensagem publicada no Kafka: %s", string(jsonOutput))
}
