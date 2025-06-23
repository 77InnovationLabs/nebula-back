package usecase

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/dto"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/repository"
	"github.com/ggialluisi/fdqf-ms/curso/pkg/event_dispatcher"
	"github.com/google/uuid"
)

type SaveCursoUseCase struct {
	CursoRepository  repository.CursoRepositoryInterface
	CursoSaved       event_dispatcher.EventInterface
	ModuloSaved      event_dispatcher.EventInterface
	AlunoSaved       event_dispatcher.EventInterface
	AlunoCursoSaved  event_dispatcher.EventInterface
	ItemModuloSaved  event_dispatcher.EventInterface
	EventDispatcher  event_dispatcher.EventDispatcherInterface
	PessoaRepository repository.PessoaRepositoryInterface
}

func NewSaveCursoUseCase(
	CursoRepository repository.CursoRepositoryInterface,
	PessoaRepository repository.PessoaRepositoryInterface,
	CursoSaved event_dispatcher.EventInterface,
	ModuloSaved event_dispatcher.EventInterface,
	AlunoSaved event_dispatcher.EventInterface,
	AlunoCursoSaved event_dispatcher.EventInterface,
	ItemModuloSaved event_dispatcher.EventInterface,
	EventDispatcher event_dispatcher.EventDispatcherInterface,
) *SaveCursoUseCase {
	return &SaveCursoUseCase{
		CursoRepository:  CursoRepository,
		PessoaRepository: PessoaRepository,
		CursoSaved:       CursoSaved,
		ModuloSaved:      ModuloSaved,
		AlunoSaved:       AlunoSaved,
		AlunoCursoSaved:  AlunoCursoSaved,
		ItemModuloSaved:  ItemModuloSaved,
		EventDispatcher:  EventDispatcher,
	}
}

// region cadastro de Curso

func (c *SaveCursoUseCase) ExecuteCreateCurso(input dto.CursoInputDTO) (dto.CursoOutputDTO, error) {

	curso, err := entity.NewCurso(
		nil,
		input.Nome,
		input.Descricao,
	)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.CreateCurso(curso)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetCurso(ret.ID)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	out_dto := dto.CursoOutputDTO{
		ID:        saved_obj.ID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}

	c.CursoSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.CursoSaved)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	return out_dto, nil
}

func (c *SaveCursoUseCase) ExecuteUpdateCurso(obj_id string, input dto.CursoInputDTO) (dto.CursoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	curso, err := entity.NewCurso(
		&obj_uuid,
		input.Nome,
		input.Descricao,
	)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.UpdateCurso(curso)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetCurso(ret.ID)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	out_dto := dto.CursoOutputDTO{
		ID:        saved_obj.ID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}
	_, err = json.Marshal(out_dto)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	c.CursoSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.CursoSaved)
	if err != nil {
		dto_nil := dto.CursoOutputDTO{}
		return dto_nil, err
	}

	return out_dto, nil
}

func (c *SaveCursoUseCase) ExecuteDeleteCurso(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.CursoRepository.DeleteCurso(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SaveCursoUseCase) ExecuteGetCurso(obj_id string) (dto.CursoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetCurso(obj_uuid)
	if err != nil {
		return dto.CursoOutputDTO{}, err
	}

	dto := dto.CursoOutputDTO{
		ID:        saved_obj.ID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteGetCursos(page, limit int, sort string) ([]dto.CursoOutputDTO, error) {
	saved_objs, err := c.CursoRepository.FindAllCursos(page, limit, sort)
	if err != nil {
		return []dto.CursoOutputDTO{}, err
	}

	var dtos []dto.CursoOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.CursoOutputDTO{
			ID:        saved_obj.ID,
			CreatedAt: saved_obj.CreatedAt,
			UpdatedAt: saved_obj.UpdatedAt,
			Nome:      saved_obj.Nome,
			Descricao: saved_obj.Descricao,
		}
		dtos = append(dtos, dto)
	}
	return dtos, nil
}

// endregion

// region cadastro de Modulo

func (c *SaveCursoUseCase) ExecuteCreateModulo(input dto.ModuloInputDTO) (dto.ModuloOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.CursoID)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	modulo, err := entity.NewModulo(
		parent_uuid,
		nil,
		input.Nome,
		input.Descricao,
	)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	ret, err := c.CursoRepository.CreateModulo(modulo)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetModulo(ret.ID)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	dto := dto.ModuloOutputDTO{
		ID:        saved_obj.ID,
		CursoID:   saved_obj.CursoID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteUpdateModulo(obj_id string, input dto.ModuloInputDTO) (dto.ModuloOutputDTO, error) {
	parent_uuid, err := uuid.Parse(input.CursoID)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	modulo, err := entity.NewModulo(
		parent_uuid,
		&obj_uuid,
		input.Nome,
		input.Descricao,
	)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	ret, err := c.CursoRepository.UpdateModulo(modulo)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetModulo(ret.ID)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	dto := dto.ModuloOutputDTO{
		ID:        saved_obj.ID,
		CursoID:   saved_obj.CursoID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteDeleteModulo(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.CursoRepository.DeleteModulo(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SaveCursoUseCase) ExecuteGetModulo(obj_id string) (dto.ModuloOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetModulo(obj_uuid)
	if err != nil {
		return dto.ModuloOutputDTO{}, err
	}

	dto := dto.ModuloOutputDTO{
		ID:        saved_obj.ID,
		CursoID:   saved_obj.CursoID,
		CreatedAt: saved_obj.CreatedAt,
		UpdatedAt: saved_obj.UpdatedAt,
		Nome:      saved_obj.Nome,
		Descricao: saved_obj.Descricao,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteGetModulosDeCurso(parent_id string) ([]dto.ModuloOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.ModuloOutputDTO{}, err
	}

	saved_objs, err := c.CursoRepository.GetModulosDeCurso(parent_uuid)
	if err != nil {
		return []dto.ModuloOutputDTO{}, err
	}

	var dtos []dto.ModuloOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.ModuloOutputDTO{
			ID:        saved_obj.ID,
			CursoID:   saved_obj.CursoID,
			CreatedAt: saved_obj.CreatedAt,
			UpdatedAt: saved_obj.UpdatedAt,
			Nome:      saved_obj.Nome,
			Descricao: saved_obj.Descricao,
		}
		dtos = append(dtos, dto)
	}

	return dtos, nil
}

// endregion

// region cadastro de Aluno

func (c *SaveCursoUseCase) ExecuteCreateAluno(input dto.AlunoNewInputDTO) (dto.AlunoOutputDTO, error) {
	pessoa_id, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	// Verifica se a pessoa já existe
	input_pessoa := dto.PessoaInputDTO{
		ID:   input.PessoaID,
		Tipo: "FISICA",
		Nome: input.Nome,
	}
	_, err = c.ExecuteCreateOrUpdatePessoa(input_pessoa)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	now := time.Now()
	item, err := entity.NewAluno(
		nil,
		pessoa_id,
		&now, 0, "n.d", entity.StatusAlunoAtivo,
		input.Wallet,
	)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.CreateAluno(item)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetAluno(ret.ID)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	out_dto := dto.AlunoOutputDTO{
		ID:          saved_obj.ID,
		CreatedAt:   saved_obj.CreatedAt,
		UpdatedAt:   saved_obj.UpdatedAt,
		PessoaID:    saved_obj.PessoaID,
		Nome:        saved_obj.Nome(),
		Wallet:      saved_obj.Wallet,
		TipoPessoa:  saved_obj.TipoPessoa(),
		DataInicio:  saved_obj.DataInicio,
		XpTotal:     saved_obj.XpTotal,
		NftId:       saved_obj.NftId,
		StatusAluno: saved_obj.StatusAluno,
	}

	c.AlunoSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.AlunoSaved)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	return out_dto, nil
}

func (c *SaveCursoUseCase) ExecuteUpdateAluno(obj_id string, input dto.AlunoInputDTO) (dto.AlunoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	pessoa_id, err := uuid.Parse(input.PessoaID)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	curso, err := entity.NewAluno(
		&obj_uuid,
		pessoa_id,
		input.DataInicio,
		input.XpTotal,
		input.NftId,
		entity.StatusAlunoAtivo,
		input.Wallet,
	)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.UpdateAluno(curso)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetAluno(ret.ID)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	out_dto := dto.AlunoOutputDTO{
		ID:          saved_obj.ID,
		CreatedAt:   saved_obj.CreatedAt,
		UpdatedAt:   saved_obj.UpdatedAt,
		PessoaID:    saved_obj.PessoaID,
		Nome:        saved_obj.Nome(),
		Wallet:      saved_obj.Wallet,
		TipoPessoa:  saved_obj.TipoPessoa(),
		DataInicio:  saved_obj.DataInicio,
		XpTotal:     saved_obj.XpTotal,
		NftId:       saved_obj.NftId,
		StatusAluno: saved_obj.StatusAluno,
	}
	_, err = json.Marshal(out_dto)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	c.AlunoSaved.SetPayload(out_dto)
	err = c.EventDispatcher.Dispatch(c.AlunoSaved)
	if err != nil {
		dto_nil := dto.AlunoOutputDTO{}
		return dto_nil, err
	}

	return out_dto, nil
}

func (c *SaveCursoUseCase) ExecuteDeleteAluno(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.CursoRepository.DeleteAluno(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}

func (c *SaveCursoUseCase) ExecuteGetAluno(obj_id string) (dto.AlunoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetAluno(obj_uuid)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	dto := dto.AlunoOutputDTO{
		ID:          saved_obj.ID,
		CreatedAt:   saved_obj.CreatedAt,
		UpdatedAt:   saved_obj.UpdatedAt,
		PessoaID:    saved_obj.PessoaID,
		Nome:        saved_obj.Nome(),
		Wallet:      saved_obj.Wallet,
		TipoPessoa:  saved_obj.TipoPessoa(),
		DataInicio:  saved_obj.DataInicio,
		XpTotal:     saved_obj.XpTotal,
		NftId:       saved_obj.NftId,
		StatusAluno: saved_obj.StatusAluno,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteGetAlunoByWallet(obj_wallet string) (dto.AlunoOutputDTO, error) {
	// obj_uuid, err := uuid.Parse(obj_id)
	// if err != nil {
	// 	return dto.AlunoOutputDTO{}, err
	// }

	saved_obj, err := c.CursoRepository.GetAlunoByWallet(obj_wallet)
	if err != nil {
		return dto.AlunoOutputDTO{}, err
	}

	dto := dto.AlunoOutputDTO{
		ID:          saved_obj.ID,
		CreatedAt:   saved_obj.CreatedAt,
		UpdatedAt:   saved_obj.UpdatedAt,
		PessoaID:    saved_obj.PessoaID,
		Nome:        saved_obj.Nome(),
		Wallet:      saved_obj.Wallet,
		TipoPessoa:  saved_obj.TipoPessoa(),
		DataInicio:  saved_obj.DataInicio,
		XpTotal:     saved_obj.XpTotal,
		NftId:       saved_obj.NftId,
		StatusAluno: saved_obj.StatusAluno,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteGetAlunos(page, limit int, sort string) ([]dto.AlunoOutputDTO, error) {
	saved_objs, err := c.CursoRepository.FindAllAlunos(page, limit, sort)
	if err != nil {
		return []dto.AlunoOutputDTO{}, err
	}

	var dtos []dto.AlunoOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.AlunoOutputDTO{
			ID:          saved_obj.ID,
			CreatedAt:   saved_obj.CreatedAt,
			UpdatedAt:   saved_obj.UpdatedAt,
			PessoaID:    saved_obj.PessoaID,
			Nome:        saved_obj.Nome(),
			Wallet:      saved_obj.Wallet,
			TipoPessoa:  saved_obj.TipoPessoa(),
			DataInicio:  saved_obj.DataInicio,
			XpTotal:     saved_obj.XpTotal,
			NftId:       saved_obj.NftId,
			StatusAluno: saved_obj.StatusAluno,
		}
		dtos = append(dtos, dto)
	}
	return dtos, nil
}

// endregion

// region cadastro de AlunoCurso
func (c *SaveCursoUseCase) ExecuteCreateAlunoCurso(input dto.AlunoCursoInputDTO) (dto.AlunoCursoOutputDTO, error) {
	alunoID, err := uuid.Parse(input.AlunoID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	cursoID, err := uuid.Parse(input.CursoID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	// Cria a matrícula
	alunoCurso, err := entity.NewAlunoCurso(
		nil,
		alunoID,
		cursoID,
	)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.CreateAlunoCurso(alunoCurso)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	// Busca todos os ItemModulo do curso para criar os AlunoCursoItemModulo
	modulos, err := c.CursoRepository.GetModulosDeCurso(cursoID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	var allItemModulos []entity.ItemModulo
	for _, modulo := range modulos {
		items, err := c.CursoRepository.FindItemModulosByModulo(modulo.ID)
		if err != nil {
			return dto.AlunoCursoOutputDTO{}, err
		}
		allItemModulos = append(allItemModulos, items...)
	}

	// Monta todos os AlunoCursoItemModulo com status inicial
	var itensToCreate []*entity.AlunoCursoItemModulo
	now := time.Now()
	for _, item := range allItemModulos {
		itensToCreate = append(itensToCreate, &entity.AlunoCursoItemModulo{
			ID:           uuid.New(),
			AlunoCursoID: ret.ID,
			ItemModuloID: item.ID,
			Status:       entity.TipoStatusItemModuloNaoIniciado,
			Progresso:    0,
			CreatedAt:    now,
			UpdatedAt:    now,
		})
	}

	if len(itensToCreate) > 0 {
		err = c.CursoRepository.CreateAlunoCursoItemModulosBatch(itensToCreate)
		if err != nil {
			return dto.AlunoCursoOutputDTO{}, err
		}
	}

	// Retorna DTO
	savedObj, err := c.CursoRepository.GetAlunoCurso(ret.ID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	dto := dto.AlunoCursoOutputDTO{
		ID:                  savedObj.ID,
		CursoID:             savedObj.CursoID,
		AlunoID:             savedObj.AlunoID,
		CreatedAt:           savedObj.CreatedAt,
		UpdatedAt:           savedObj.UpdatedAt,
		AlunoNome:           savedObj.Aluno.Nome(),
		CursoNome:           savedObj.Curso.Nome,
		CursoDescricao:      savedObj.Curso.Descricao,
		DataMatricula:       savedObj.DataMatricula,
		PercentualConcluido: savedObj.PercentualConcluido,
		StatusCurso:         savedObj.StatusCurso,
		StatusPagamento:     savedObj.StatusPagamento,
	}

	return dto, nil
}

func (c *SaveCursoUseCase) ExecuteUpdateAlunoCurso(obj_id string, input dto.AlunoCursoInputDTO) (dto.AlunoCursoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	aluno_id, err := uuid.Parse(input.AlunoID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	curso_id, err := uuid.Parse(input.CursoID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	alunocurso, err := entity.NewAlunoCurso(
		&obj_uuid,
		curso_id,
		aluno_id,
	)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	ret, err := c.CursoRepository.UpdateAlunoCurso(alunocurso)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetAlunoCurso(ret.ID)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	dto := dto.AlunoCursoOutputDTO{
		ID:                  saved_obj.ID,
		CursoID:             saved_obj.CursoID,
		AlunoID:             saved_obj.AlunoID,
		CreatedAt:           saved_obj.CreatedAt,
		UpdatedAt:           saved_obj.UpdatedAt,
		AlunoNome:           saved_obj.Aluno.Nome(),
		CursoNome:           saved_obj.Curso.Nome,
		CursoDescricao:      saved_obj.Curso.Descricao,
		DataMatricula:       saved_obj.DataMatricula,
		PercentualConcluido: saved_obj.PercentualConcluido,
		StatusCurso:         saved_obj.StatusCurso,
		StatusPagamento:     saved_obj.StatusPagamento,
	}

	return dto, nil
}
func (c *SaveCursoUseCase) ExecuteDeleteAlunoCurso(obj_id string) error {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}

	err = c.CursoRepository.DeleteAlunoCurso(obj_uuid)
	if err != nil {
		return err
	}

	//to-do - aqui precisa disparar um evento para o log

	return nil
}
func (c *SaveCursoUseCase) ExecuteGetAlunoCurso(obj_id string) (dto.AlunoCursoOutputDTO, error) {
	obj_uuid, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	saved_obj, err := c.CursoRepository.GetAlunoCurso(obj_uuid)
	if err != nil {
		return dto.AlunoCursoOutputDTO{}, err
	}

	dto := dto.AlunoCursoOutputDTO{
		ID:                  saved_obj.ID,
		CursoID:             saved_obj.CursoID,
		AlunoID:             saved_obj.AlunoID,
		CreatedAt:           saved_obj.CreatedAt,
		UpdatedAt:           saved_obj.UpdatedAt,
		AlunoNome:           saved_obj.Aluno.Nome(),
		CursoNome:           saved_obj.Curso.Nome,
		CursoDescricao:      saved_obj.Curso.Descricao,
		DataMatricula:       saved_obj.DataMatricula,
		PercentualConcluido: saved_obj.PercentualConcluido,
		StatusCurso:         saved_obj.StatusCurso,
		StatusPagamento:     saved_obj.StatusPagamento,
	}

	return dto, nil
}
func (c *SaveCursoUseCase) ExecuteGetAlunosDoCurso(parent_id string) ([]dto.AlunoCursoOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.AlunoCursoOutputDTO{}, err
	}

	saved_objs, err := c.CursoRepository.FindAlunosDoCurso(parent_uuid)
	if err != nil {
		return []dto.AlunoCursoOutputDTO{}, err
	}

	var dtos []dto.AlunoCursoOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.AlunoCursoOutputDTO{
			ID:                  saved_obj.ID,
			CursoID:             saved_obj.CursoID,
			AlunoID:             saved_obj.AlunoID,
			CreatedAt:           saved_obj.CreatedAt,
			UpdatedAt:           saved_obj.UpdatedAt,
			AlunoNome:           saved_obj.Aluno.Nome(),
			CursoNome:           saved_obj.Curso.Nome,
			CursoDescricao:      saved_obj.Curso.Descricao,
			DataMatricula:       saved_obj.DataMatricula,
			PercentualConcluido: saved_obj.PercentualConcluido,
			StatusCurso:         saved_obj.StatusCurso,
			StatusPagamento:     saved_obj.StatusPagamento,
		}

		dtos = append(dtos, dto)
	}

	return dtos, nil
}
func (c *SaveCursoUseCase) ExecuteGetCursosDoAluno(parent_id string) ([]dto.AlunoCursoOutputDTO, error) {
	parent_uuid, err := uuid.Parse(parent_id)
	if err != nil {
		return []dto.AlunoCursoOutputDTO{}, err
	}

	saved_objs, err := c.CursoRepository.FindCursosDoAluno(parent_uuid)
	if err != nil {
		return []dto.AlunoCursoOutputDTO{}, err
	}

	var dtos []dto.AlunoCursoOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.AlunoCursoOutputDTO{
			ID:                  saved_obj.ID,
			CursoID:             saved_obj.CursoID,
			AlunoID:             saved_obj.AlunoID,
			CreatedAt:           saved_obj.CreatedAt,
			UpdatedAt:           saved_obj.UpdatedAt,
			AlunoNome:           saved_obj.Aluno.Nome(),
			CursoNome:           saved_obj.Curso.Nome,
			CursoDescricao:      saved_obj.Curso.Descricao,
			DataMatricula:       saved_obj.DataMatricula,
			PercentualConcluido: saved_obj.PercentualConcluido,
			StatusCurso:         saved_obj.StatusCurso,
			StatusPagamento:     saved_obj.StatusPagamento,
		}
		dtos = append(dtos, dto)
	}

	return dtos, nil
}
func (c *SaveCursoUseCase) ExecuteGetAlunoCursos(page, limit int, sort string) ([]dto.AlunoCursoOutputDTO, error) {
	saved_objs, err := c.CursoRepository.FindAllAlunoCursos(page, limit, sort)
	if err != nil {
		return []dto.AlunoCursoOutputDTO{}, err
	}

	var dtos []dto.AlunoCursoOutputDTO
	for _, saved_obj := range saved_objs {
		dto := dto.AlunoCursoOutputDTO{
			ID:                  saved_obj.ID,
			CursoID:             saved_obj.CursoID,
			AlunoID:             saved_obj.AlunoID,
			CreatedAt:           saved_obj.CreatedAt,
			UpdatedAt:           saved_obj.UpdatedAt,
			AlunoNome:           saved_obj.Aluno.Nome(),
			CursoNome:           saved_obj.Curso.Nome,
			CursoDescricao:      saved_obj.Curso.Descricao,
			DataMatricula:       saved_obj.DataMatricula,
			PercentualConcluido: saved_obj.PercentualConcluido,
			StatusCurso:         saved_obj.StatusCurso,
			StatusPagamento:     saved_obj.StatusPagamento,
		}
		dtos = append(dtos, dto)
	}

	return dtos, nil
}

// endregion

// region cadastro de ItemModulo

func (c *SaveCursoUseCase) ExecuteCreateItemModulo(input dto.ItemModuloInputDTO) (dto.ItemModuloOutputDTO, error) {
	moduloID, err := uuid.Parse(input.ModuloID)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	maxOrdem, err := c.CursoRepository.GetMaxOrdemItemModulo(moduloID)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	// Cria o item
	item := &entity.ItemModulo{
		ID:                 uuid.New(),
		ModuloID:           moduloID,
		Nome:               input.Nome,
		Descricao:          input.Descricao,
		EstimativaTempoMin: input.EstimativaTempoMin,
		Tipo:               entity.TipoItem(input.Tipo),
		CreatedAt:          time.Now(),
		UpdatedAt:          time.Now(),
		Ordem:              maxOrdem + 1,
	}

	switch item.Tipo {
	case entity.ItemAula:
		if input.Aula != nil {
			item.Aula = &entity.ItemModuloAula{
				ItemModuloID: item.ID,
				Texto:        input.Aula.Texto,
			}
		}
	case entity.ItemContractValidate:
		if input.ContractValidation != nil {
			item.ContractValidation = &entity.ItemModuloContractValidation{
				ItemModuloID:     item.ID,
				Rede:             entity.RedeValidacao(input.ContractValidation.Rede),
				EnderecoContrato: input.ContractValidation.EnderecoContrato,
			}
		}
	}

	err = c.CursoRepository.CreateItemModulo(item)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	return toOutputDTO(item), nil
}

func (c *SaveCursoUseCase) ExecuteFindItemModuloByID(obj_id string) (dto.ItemModuloOutputDTO, error) {
	itemID, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}
	item, err := c.CursoRepository.FindItemModuloByID(itemID)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}
	return toOutputDTO(item), nil
}

func (c *SaveCursoUseCase) ExecuteFindItemModulosByModulo(parent_id string) ([]dto.ItemModuloOutputDTO, error) {
	modID, err := uuid.Parse(parent_id)
	if err != nil {
		return nil, err
	}
	itens, err := c.CursoRepository.FindItemModulosByModulo(modID)
	if err != nil {
		return nil, err
	}
	var output []dto.ItemModuloOutputDTO
	for _, item := range itens {
		output = append(output, toOutputDTO(&item))
	}
	return output, nil
}

func (c *SaveCursoUseCase) ExecuteUpdateItemModulo(obj_id string, input dto.ItemModuloInputDTO) (dto.ItemModuloOutputDTO, error) {
	itemID, err := uuid.Parse(obj_id)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}
	moduloID, err := uuid.Parse(input.ModuloID)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	item, err := c.CursoRepository.FindItemModuloByID(itemID)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	item.ModuloID = moduloID
	item.Nome = input.Nome
	item.Descricao = input.Descricao
	item.EstimativaTempoMin = input.EstimativaTempoMin
	item.Tipo = entity.TipoItem(input.Tipo)
	item.UpdatedAt = time.Now()

	switch item.Tipo {
	case entity.ItemAula:
		item.Aula = &entity.ItemModuloAula{
			ItemModuloID: item.ID,
			Texto:        input.Aula.Texto,
		}
	case entity.ItemContractValidate:
		item.ContractValidation = &entity.ItemModuloContractValidation{
			ItemModuloID:     item.ID,
			Rede:             entity.RedeValidacao(input.ContractValidation.Rede),
			EnderecoContrato: input.ContractValidation.EnderecoContrato,
		}
	}

	err = c.CursoRepository.UpdateItemModulo(item)
	if err != nil {
		return dto.ItemModuloOutputDTO{}, err
	}

	return toOutputDTO(item), nil
}

func (c *SaveCursoUseCase) ExecuteDeleteItemModulo(obj_id string) error {
	itemID, err := uuid.Parse(obj_id)
	if err != nil {
		return err
	}
	return c.CursoRepository.DeleteItemModulo(itemID)
}

func (c *SaveCursoUseCase) ExecuteMoveItemModulo(id uuid.UUID, action string) error {
	return c.CursoRepository.MoveItemModulo(id, action)
}

// Helper
func toOutputDTO(item *entity.ItemModulo) dto.ItemModuloOutputDTO {
	out := dto.ItemModuloOutputDTO{
		ID:                 item.ID.String(),
		ModuloID:           item.ModuloID.String(),
		Nome:               item.Nome,
		Descricao:          item.Descricao,
		EstimativaTempoMin: item.EstimativaTempoMin,
		Tipo:               string(item.Tipo),
		Ordem:              item.Ordem,
		CreatedAt:          item.CreatedAt,
		UpdatedAt:          item.UpdatedAt,
	}
	if item.Aula != nil {
		out.Aula = &dto.ItemModuloAulaDTO{Texto: item.Aula.Texto}
	}
	if item.ContractValidation != nil {
		out.ContractValidation = &dto.ItemModuloContractValidationDTO{
			Rede:             string(item.ContractValidation.Rede),
			EnderecoContrato: item.ContractValidation.EnderecoContrato,
		}
	}
	return out
}

// endregion

// region cadastro de Pessoa
func (c *SaveCursoUseCase) ExecuteCreateOrUpdatePessoa(input dto.PessoaInputDTO) (dto.PessoaOutputDTO, error) {
	id, err := uuid.Parse(input.ID)
	if err != nil {
		return dto.PessoaOutputDTO{}, err
	}

	//print o nome
	fmt.Printf("Saving Pessoa - input.Nome: %s", input.Nome)

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

// endregion

// region AlunoCursoItemModulo

// ExecuteFindAlunoCursoItemModulos lista todos os itens de um AlunoCurso
func (c *SaveCursoUseCase) ExecuteFindAlunoCursoItemModulos(alunoCursoID string) ([]dto.AlunoCursoItemModuloResponseDTO, error) {
	alunoCursoUUID, err := uuid.Parse(alunoCursoID)
	if err != nil {
		return nil, err
	}

	itens, err := c.CursoRepository.FindItemModulosByAlunoCurso(alunoCursoUUID)
	if err != nil {
		return nil, err
	}

	var output []dto.AlunoCursoItemModuloResponseDTO
	for _, item := range itens {
		output = append(output, dto.AlunoCursoItemModuloResponseDTO{
			ID:                      item.ID,
			AlunoCursoID:            item.AlunoCursoID,
			ItemModuloID:            item.ItemModuloID,
			ItemModuloNome:          item.ItemModulo.Nome,
			TipoItemModulo:          item.ItemModulo.Tipo,
			Status:                  item.Status,
			Progresso:               item.Progresso,
			TempoAssistido:          item.TempoAssistido,
			EnderecoContratoValidar: item.EnderecoContratoValidar,
			BlockchainRedeValidacao: item.BlockchainRedeValidacao,
			BlockchainTxEnvio:       item.BlockchainTxEnvio,
			StatusValidacaoContrato: item.StatusValidacaoContrato,
			CreatedAt:               item.CreatedAt,
			UpdatedAt:               item.UpdatedAt,
		})
	}

	return output, nil
}

// ExecuteGetAlunoCursoItemModulo busca um único AlunoCursoItemModulo
func (c *SaveCursoUseCase) ExecuteGetAlunoCursoItemModulo(id string) (dto.AlunoCursoItemModuloResponseDTO, error) {
	itemID, err := uuid.Parse(id)
	if err != nil {
		return dto.AlunoCursoItemModuloResponseDTO{}, err
	}

	item, err := c.CursoRepository.GetAlunoCursoItemModulo(itemID)
	if err != nil {
		return dto.AlunoCursoItemModuloResponseDTO{}, err
	}

	output := dto.AlunoCursoItemModuloResponseDTO{
		ID:                      item.ID,
		AlunoCursoID:            item.AlunoCursoID,
		ItemModuloID:            item.ItemModuloID,
		ItemModuloNome:          item.ItemModulo.Nome,
		TipoItemModulo:          item.ItemModulo.Tipo,
		Status:                  item.Status,
		Progresso:               item.Progresso,
		TempoAssistido:          item.TempoAssistido,
		EnderecoContratoValidar: item.EnderecoContratoValidar,
		BlockchainRedeValidacao: item.BlockchainRedeValidacao,
		BlockchainTxEnvio:       item.BlockchainTxEnvio,
		StatusValidacaoContrato: item.StatusValidacaoContrato,
		CreatedAt:               item.CreatedAt,
		UpdatedAt:               item.UpdatedAt,
	}

	return output, nil
}

// ExecuteUpdateAlunoCursoItemModulo atualiza campos do AlunoCursoItemModulo
func (c *SaveCursoUseCase) ExecuteUpdateAlunoCursoItemModulo(id string, input dto.AlunoCursoItemModuloUpdateDTO) (dto.AlunoCursoItemModuloResponseDTO, error) {
	itemID, err := uuid.Parse(id)
	if err != nil {
		return dto.AlunoCursoItemModuloResponseDTO{}, err
	}

	item, err := c.CursoRepository.GetAlunoCursoItemModulo(itemID)
	if err != nil {
		return dto.AlunoCursoItemModuloResponseDTO{}, err
	}

	// Aplicar apenas os campos não-nulos
	if input.Status != nil {
		item.Status = *input.Status
	}
	if input.Progresso != nil {
		item.Progresso = *input.Progresso
	}
	if input.TempoAssistido != nil {
		item.TempoAssistido = *input.TempoAssistido
	}
	if input.EnderecoContratoValidar != nil {
		item.EnderecoContratoValidar = *input.EnderecoContratoValidar
	}
	if input.BlockchainRedeValidacao != nil {
		item.BlockchainRedeValidacao = *input.BlockchainRedeValidacao
	}
	if input.BlockchainTxEnvio != nil {
		item.BlockchainTxEnvio = *input.BlockchainTxEnvio
	}
	if input.StatusValidacaoContrato != nil {
		item.StatusValidacaoContrato = *input.StatusValidacaoContrato
	}

	// Atualiza timestamp
	item.UpdatedAt = time.Now()

	err = c.CursoRepository.UpdateAlunoCursoItemModulo(item)
	if err != nil {
		return dto.AlunoCursoItemModuloResponseDTO{}, err
	}

	output := dto.AlunoCursoItemModuloResponseDTO{
		ID:                      item.ID,
		AlunoCursoID:            item.AlunoCursoID,
		ItemModuloID:            item.ItemModuloID,
		ItemModuloNome:          item.ItemModulo.Nome,
		TipoItemModulo:          item.ItemModulo.Tipo,
		Status:                  item.Status,
		Progresso:               item.Progresso,
		TempoAssistido:          item.TempoAssistido,
		EnderecoContratoValidar: item.EnderecoContratoValidar,
		BlockchainRedeValidacao: item.BlockchainRedeValidacao,
		BlockchainTxEnvio:       item.BlockchainTxEnvio,
		StatusValidacaoContrato: item.StatusValidacaoContrato,
		CreatedAt:               item.CreatedAt,
		UpdatedAt:               item.UpdatedAt,
	}

	return output, nil
}

// endregion
