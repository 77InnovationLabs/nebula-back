package usecase

import (
	"encoding/json"

	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/dto"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/repository"
	"github.com/77InnovationLabs/nebula-back/pessoa/pkg/event_dispatcher"
	"github.com/google/uuid"
)

type SavePessoaUseCase struct {
	PessoaRepository repository.PessoaRepositoryInterface
	PessoaSaved      event_dispatcher.EventInterface
	EventDispatcher  event_dispatcher.EventDispatcherInterface
}

func NewSavePessoaUseCase(
	PessoaRepository repository.PessoaRepositoryInterface,
	PessoaSaved event_dispatcher.EventInterface,
	EventDispatcher event_dispatcher.EventDispatcherInterface,
) *SavePessoaUseCase {
	return &SavePessoaUseCase{
		PessoaRepository: PessoaRepository,
		PessoaSaved:      PessoaSaved,
		EventDispatcher:  EventDispatcher,
	}
}

// region cadastro de Pessoa
func (c *SavePessoaUseCase) ExecuteCreatePessoaNomeEmail(input dto.PessoaNomeEmailInputDTO) (dto.PessoaOutputDTO, error) {
	pessoa, err := entity.NewPessoa(
		nil,
		entity.PessoaFisica,
		input.Nome,
		"n.d",
	)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.CreatePessoa(pessoa)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	if input.Email != "" {
		// Cria o email associado à pessoa
		email, err := entity.NewEmail(
			ret.ID,
			nil,
			input.Email,
			true, // é principal por padrão
		)
		if err != nil {
			return dto.PessoaOutputDTO{}, err
		}

		_, err = c.PessoaRepository.CreateEmail(email)
		if err != nil {
			return dto.PessoaOutputDTO{}, err
		}
	}

	// retorna o objeto salvo
	saved_obj, err := c.PessoaRepository.GetPessoa(ret.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	out_dto := dto.PessoaOutputDTO{
		ID:        saved_obj.ID,
		Tipo:      saved_obj.Tipo,
		Nome:      saved_obj.Nome,
		Documento: saved_obj.Documento,
	}

	c.PessoaSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.PessoaSaved)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteCreatePessoa(input dto.PessoaInputDTO) (dto.PessoaOutputDTO, error) {
	pessoa, err := entity.NewPessoa(
		nil,
		input.Tipo,
		input.Nome,
		input.Documento,
	)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.CreatePessoa(pessoa)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetPessoa(ret.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	out_dto := dto.PessoaOutputDTO{
		ID:        saved_obj.ID,
		Tipo:      saved_obj.Tipo,
		Nome:      saved_obj.Nome,
		Documento: saved_obj.Documento,
	}

	c.PessoaSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.PessoaSaved)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteUpdatePessoa(obj_id string, input dto.PessoaInputDTO) (dto.PessoaOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	pessoa, err := entity.NewPessoa(
		&obj_uuid,
		input.Tipo,
		input.Nome,
		input.Documento,
	)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.UpdatePessoa(pessoa)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetPessoa(ret.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	out_dto := dto.PessoaOutputDTO{
		ID:        saved_obj.ID,
		Tipo:      saved_obj.Tipo,
		Nome:      saved_obj.Nome,
		Documento: saved_obj.Documento,
	}

	_, err = json.Marshal(out_dto)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	c.PessoaSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.PessoaSaved)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
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

	//to-do - aqui precisa disparar um evento para o log

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
		ID:        saved_obj.ID,
		Tipo:      saved_obj.Tipo,
		Nome:      saved_obj.Nome,
		Documento: saved_obj.Documento,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteGetPessoas(page, limit int, sort string) ([]dto.PessoaOutputDTO, error) {
	saved_objs, err := c.PessoaRepository.FindAllPessoas(page, limit, sort)
	if err != nil {
		return []dto.PessoaOutputDTO{}, err
	}

	var dtos []dto.PessoaOutputDTO
	for _, saved_obj := range saved_objs {
		out_dto := dto.PessoaOutputDTO{
			ID:        saved_obj.ID,
			Tipo:      saved_obj.Tipo,
			Nome:      saved_obj.Nome,
			Documento: saved_obj.Documento,
		}
		dtos = append(dtos, out_dto)
	}
	return dtos, nil
}

// endregion

// region cadastro de Endereco

func (c *SavePessoaUseCase) ExecuteCreateEndereco(input dto.EnderecoInputDTO) (dto.EnderecoOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	endereco, err := entity.NewEndereco(
		parent_uuid,
		nil,
		input.Logradouro,
		input.Numero,
		input.CEP,
		input.Bairro,
		input.Cidade,
		input.Estado,
		input.Principal,
		input.SemNumero,
	)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.CreateEndereco(endereco)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEndereco(ret.ID)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	out_dto := dto.EnderecoOutputDTO{
		ID:         saved_obj.ID,
		PessoaID:   saved_obj.PessoaID,
		Logradouro: saved_obj.Logradouro,
		Numero:     saved_obj.Numero,
		CEP:        saved_obj.CEP,
		Bairro:     saved_obj.Bairro,
		Cidade:     saved_obj.Cidade,
		Estado:     saved_obj.Estado,
		Principal:  saved_obj.Principal,
		SemNumero:  saved_obj.SemNumero,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteUpdateEndereco(obj_id string, input dto.EnderecoInputDTO) (dto.EnderecoOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	endereco, err := entity.NewEndereco(
		parent_uuid,
		&obj_uuid,
		input.Logradouro,
		input.Numero,
		input.CEP,
		input.Bairro,
		input.Cidade,
		input.Estado,
		input.Principal,
		input.SemNumero,
	)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.UpdateEndereco(endereco)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEndereco(ret.ID)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	out_dto := dto.EnderecoOutputDTO{
		ID:         saved_obj.ID,
		PessoaID:   saved_obj.PessoaID,
		Logradouro: saved_obj.Logradouro,
		Numero:     saved_obj.Numero,
		CEP:        saved_obj.CEP,
		Bairro:     saved_obj.Bairro,
		Cidade:     saved_obj.Cidade,
		Estado:     saved_obj.Estado,
		Principal:  saved_obj.Principal,
		SemNumero:  saved_obj.SemNumero,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteDeleteEndereco(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.PessoaRepository.DeleteEndereco(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SavePessoaUseCase) ExecuteGetEndereco(obj_id string) (dto.EnderecoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEndereco(obj_uuid)
	if err != nil {
		return dto.EnderecoOutputDTO{}, err
	}

	out_dto := dto.EnderecoOutputDTO{
		ID:         saved_obj.ID,
		PessoaID:   saved_obj.PessoaID,
		Logradouro: saved_obj.Logradouro,
		Numero:     saved_obj.Numero,
		CEP:        saved_obj.CEP,
		Bairro:     saved_obj.Bairro,
		Cidade:     saved_obj.Cidade,
		Estado:     saved_obj.Estado,
		Principal:  saved_obj.Principal,
		SemNumero:  saved_obj.SemNumero,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteGetEnderecosDaPessoa(parent_id string) ([]dto.EnderecoOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.EnderecoOutputDTO{}, err
	}

	saved_objs, err := c.PessoaRepository.GetEnderecosDaPessoa(parent_uuid)
	if err != nil {
		return []dto.EnderecoOutputDTO{}, err
	}

	var dtos []dto.EnderecoOutputDTO
	for _, saved_obj := range saved_objs {
		out_dto := dto.EnderecoOutputDTO{
			ID:         saved_obj.ID,
			PessoaID:   saved_obj.PessoaID,
			Logradouro: saved_obj.Logradouro,
			Numero:     saved_obj.Numero,
			CEP:        saved_obj.CEP,
			Bairro:     saved_obj.Bairro,
			Cidade:     saved_obj.Cidade,
			Estado:     saved_obj.Estado,
			Principal:  saved_obj.Principal,
			SemNumero:  saved_obj.SemNumero,
		}
		dtos = append(dtos, out_dto)
	}

	return dtos, nil
}

// endregion

// region cadastro de Telefone

func (c *SavePessoaUseCase) ExecuteCreateTelefone(input dto.TelefoneInputDTO) (dto.TelefoneOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	telefone, err := entity.NewTelefone(
		parent_uuid,
		nil,
		input.DDD,
		input.Numero,
		input.Principal,
	)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.CreateTelefone(telefone)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetTelefone(ret.ID)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	out_dto := dto.TelefoneOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		DDD:       saved_obj.DDD,
		Numero:    saved_obj.Numero,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteUpdateTelefone(obj_id string, input dto.TelefoneInputDTO) (dto.TelefoneOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	telefone, err := entity.NewTelefone(
		parent_uuid,
		&obj_uuid,
		input.DDD,
		input.Numero,
		input.Principal,
	)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.UpdateTelefone(telefone)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetTelefone(ret.ID)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	out_dto := dto.TelefoneOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		DDD:       saved_obj.DDD,
		Numero:    saved_obj.Numero,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteDeleteTelefone(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.PessoaRepository.DeleteTelefone(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SavePessoaUseCase) ExecuteGetTelefone(obj_id string) (dto.TelefoneOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetTelefone(obj_uuid)
	if err != nil {
		return dto.TelefoneOutputDTO{}, err
	}

	out_dto := dto.TelefoneOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		DDD:       saved_obj.DDD,
		Numero:    saved_obj.Numero,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteGetTelefonesDaPessoa(parent_id string) ([]dto.TelefoneOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.TelefoneOutputDTO{}, err
	}

	saved_objs, err := c.PessoaRepository.GetTelefonesDaPessoa(parent_uuid)
	if err != nil {
		return []dto.TelefoneOutputDTO{}, err
	}

	var dtos []dto.TelefoneOutputDTO
	for _, saved_obj := range saved_objs {
		out_dto := dto.TelefoneOutputDTO{
			ID:        saved_obj.ID,
			PessoaID:  saved_obj.PessoaID,
			DDD:       saved_obj.DDD,
			Numero:    saved_obj.Numero,
			Principal: saved_obj.Principal,
		}
		dtos = append(dtos, out_dto)
	}

	return dtos, nil
}

// endregion

// region cadastro de Email

func (c *SavePessoaUseCase) ExecuteCreateEmail(input dto.EmailInputDTO) (dto.EmailOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	email, err := entity.NewEmail(
		parent_uuid,
		nil,
		input.Endereco,
		input.Principal,
	)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.CreateEmail(email)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEmail(ret.ID)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	out_dto := dto.EmailOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		Endereco:  saved_obj.Endereco,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteUpdateEmail(obj_id string, input dto.EmailInputDTO) (dto.EmailOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	email, err := entity.NewEmail(
		parent_uuid,
		&obj_uuid,
		input.Endereco,
		input.Principal,
	)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	ret, err := c.PessoaRepository.UpdateEmail(email)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEmail(ret.ID)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	out_dto := dto.EmailOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		Endereco:  saved_obj.Endereco,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteDeleteEmail(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.PessoaRepository.DeleteEmail(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SavePessoaUseCase) ExecuteGetEmail(obj_id string) (dto.EmailOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	saved_obj, err := c.PessoaRepository.GetEmail(obj_uuid)
	if err != nil {
		return dto.EmailOutputDTO{}, err
	}

	out_dto := dto.EmailOutputDTO{
		ID:        saved_obj.ID,
		PessoaID:  saved_obj.PessoaID,
		Endereco:  saved_obj.Endereco,
		Principal: saved_obj.Principal,
	}

	return out_dto, nil
}

func (c *SavePessoaUseCase) ExecuteGetEmailsDaPessoa(parent_id string) ([]dto.EmailOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.EmailOutputDTO{}, err
	}

	saved_objs, err := c.PessoaRepository.GetEmailsDaPessoa(parent_uuid)
	if err != nil {
		return []dto.EmailOutputDTO{}, err
	}

	var dtos []dto.EmailOutputDTO
	for _, saved_obj := range saved_objs {
		out_dto := dto.EmailOutputDTO{
			ID:        saved_obj.ID,
			PessoaID:  saved_obj.PessoaID,
			Endereco:  saved_obj.Endereco,
			Principal: saved_obj.Principal,
		}
		dtos = append(dtos, out_dto)
	}

	return dtos, nil
}

// endregion
