package usecase

import (
	"time"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/dto"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/repository"
	"github.com/google/uuid"
)

type SavePessoaUseCase struct {
	PessoaRepository repository.PessoaRepositoryInterface
}

func NewSavePessoaUseCase(
	PessoaRepository repository.PessoaRepositoryInterface,
) *SavePessoaUseCase {
	return &SavePessoaUseCase{
		PessoaRepository: PessoaRepository,
	}
}

// region cadastro de Pessoa

func (c *SavePessoaUseCase) ExecuteCreateOrUpdatePessoa(input dto.PessoaInputDTO) (dto.PessoaOutputDTO, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	pessoa, err := entity.NewPessoa(
		&id,
		entity.TipoPessoa(input.Tipo),
		input.Nome,
	)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	// seta created_at e updated_at
	pessoa.CreatedAt = time.Now()
	pessoa.UpdatedAt = time.Now()

	var ret *entity.Pessoa
	p, err := c.PessoaRepository.GetPessoa(pessoa.ID)
	if err != nil {
		//vai criar
		ret, err = c.PessoaRepository.CreatePessoa(pessoa)
		if err != nil {
			return dto.PessoaOutputDTO{}, err
		}
	} else {
		//vai atualizar
		pessoa.CreatedAt = p.CreatedAt
		ret, err = c.PessoaRepository.UpdatePessoa(pessoa)
		if err != nil {
			return dto.PessoaOutputDTO{}, err
		}
	}

	saved_obj, err := c.PessoaRepository.GetPessoa(ret.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	out_dto := dto.PessoaOutputDTO{
		ID:   saved_obj.ID,
		Tipo: string(saved_obj.Tipo),
		Nome: saved_obj.Nome,
	}
	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteDeletePessoa(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.PessoaRepository.DeletePessoa(obj_uuid)
	if err != nil {
		return err
	}

	return nil
}

func (c *SavePessoaUseCase) ExecuteGetPessoa(obj_id string) (dto.PessoaOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetPessoa(obj_uuid)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	out_dto := dto.PessoaOutputDTO{
		ID:   saved_obj.ID,
		Tipo: string(saved_obj.Tipo),
		Nome: saved_obj.Nome,
	}
	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteGetPessoas() ([]dto.PessoaOutputDTO, error) {
	saved_objs, err := c.PessoaRepository.GetPessoas()
	if err != nil {
		return nil, err
	}

	var dtos []dto.PessoaOutputDTO
	for _, saved_obj := range saved_objs {
		out_dto := dto.PessoaOutputDTO{
			ID:   saved_obj.ID,
			Tipo: string(saved_obj.Tipo),
			Nome: saved_obj.Nome,
		}
		dtos = append(dtos, out_dto)
	}
	return dtos, nil
}

// endregion
