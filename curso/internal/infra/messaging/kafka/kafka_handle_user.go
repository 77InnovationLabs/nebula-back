package kafka

import (
	"encoding/json"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/dto"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/repository"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/usecase"
	"github.com/segmentio/kafka-go"
)

type PessoaKafkaHandlers struct {
	PessoaRepository repository.PessoaRepositoryInterface
}

func NewPessoaKafkaHandlers(
	PessoaRepository repository.PessoaRepositoryInterface,
) *PessoaKafkaHandlers {
	return &PessoaKafkaHandlers{
		PessoaRepository: PessoaRepository,
	}
}

func (h *PessoaKafkaHandlers) CreateOrUpdatePessoa(m kafka.Message) error {
	var inputDto dto.PessoaInputDTO
	err := json.Unmarshal(m.Value, &inputDto)
	if err != nil {
		return err
	}

	uc := usecase.NewSavePessoaUseCase(h.PessoaRepository)
	_, err = uc.ExecuteCreateOrUpdatePessoa(inputDto)
	if err != nil {
		return err
	}

	return nil
}
