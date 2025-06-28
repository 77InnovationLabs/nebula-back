package gorm

import (
	"errors"

	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/entity"
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/repository"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Verifica se essa IMPLEMENTAÇÃO implementa corretamente a INTERFACE
var _ repository.CursoRepositoryInterface = &CursoRepositoryGorm{}

type CursoRepositoryGorm struct {
	DB *gorm.DB
}

func NewCursoRepositoryGorm(db *gorm.DB) *CursoRepositoryGorm {
	return &CursoRepositoryGorm{DB: db}
}

// region CRUD Curso

func (r *CursoRepositoryGorm) CreateCurso(obj *entity.Curso) (*entity.Curso, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) UpdateCurso(obj *entity.Curso) (*entity.Curso, error) {
	s, err := r.GetCurso(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) DeleteCurso(objID uuid.UUID) error {
	obj, err := r.GetCurso(objID)
	if err != nil {
		return err
	}
	return r.DB.Delete(&entity.Curso{}, obj.ID).Error
}

func (r *CursoRepositoryGorm) GetCurso(objID uuid.UUID) (*entity.Curso, error) {
	var obj entity.Curso
	err := r.DB.Preload("Modulos").Where("id = ?", objID.String()).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *CursoRepositoryGorm) GetCursoByDocumento(documento string) (*entity.Curso, error) {
	var curso entity.Curso
	err := r.DB.Where("documento = ?", documento).First(&curso).Error
	if err != nil {
		return nil, err
	}
	return &curso, nil
}

func (r *CursoRepositoryGorm) FindAllCursos(page, limit int, sort string) ([]entity.Curso, error) {
	var itens []entity.Curso
	var err error
	if sort != "" && sort != "asc" && sort != "desc" {
		sort = "asc"
	}
	preloads := r.DB.Preload("Modulos")
	if page != 0 && limit != 0 {
		err = preloads.Limit(limit).Offset((page - 1) * limit).Order("created_at " + sort).Find(&itens).Error
	} else {
		err = preloads.Order("created_at " + sort).Find(&itens).Error
	}
	return itens, err
}

// endregion

// region CRUD Modulo

func (r *CursoRepositoryGorm) CreateModulo(obj *entity.Modulo) (*entity.Modulo, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) GetModulo(objID uuid.UUID) (*entity.Modulo, error) {
	var obj entity.Modulo
	err := r.DB.Where("id = ?", objID).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *CursoRepositoryGorm) UpdateModulo(obj *entity.Modulo) (*entity.Modulo, error) {
	s, err := r.GetModulo(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) DeleteModulo(objID uuid.UUID) error {
	return r.DB.Delete(&entity.Modulo{}, objID).Error
}

func (r *CursoRepositoryGorm) GetModulosDeCurso(parentID uuid.UUID) ([]entity.Modulo, error) {
	var itens []entity.Modulo
	err := r.DB.Where("curso_id = ?", parentID).Find(&itens).Error
	return itens, err
}

// endregion

// region CRUD Aluno

func (r *CursoRepositoryGorm) CreateAluno(obj *entity.Aluno) (*entity.Aluno, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) UpdateAluno(obj *entity.Aluno) (*entity.Aluno, error) {
	s, err := r.GetAluno(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *CursoRepositoryGorm) DeleteAluno(objID uuid.UUID) error {
	obj, err := r.GetAluno(objID)
	if err != nil {
		return err
	}
	return r.DB.Delete(&entity.Aluno{}, obj.ID).Error
}

func (r *CursoRepositoryGorm) GetAluno(objID uuid.UUID) (*entity.Aluno, error) {
	var obj entity.Aluno
	err := r.DB. /*.Preload("Modulos")*/ Preload("Pessoa").Where("id = ?", objID.String()).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *CursoRepositoryGorm) GetAlunoByWallet(wallet string) (*entity.Aluno, error) {
	var obj entity.Aluno
	err := r.DB.Preload("Pessoa").Where("wallet = ?", wallet).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *CursoRepositoryGorm) GetAlunoByDocumento(documento string) (*entity.Aluno, error) {
	var aluno entity.Aluno
	err := r.DB.Where("documento = ?", documento).First(&aluno).Error
	if err != nil {
		return nil, err
	}
	return &aluno, nil
}

func (r *CursoRepositoryGorm) FindAllAlunos(page, limit int, sort string) ([]entity.Aluno, error) {
	var itens []entity.Aluno
	var err error
	if sort != "" && sort != "asc" && sort != "desc" {
		sort = "asc"
	}
	preloads := r.DB.Preload("Pessoa")
	if page != 0 && limit != 0 {
		err = preloads.Limit(limit).Offset((page - 1) * limit).Order("created_at " + sort).Find(&itens).Error
	} else {
		err = preloads.Order("created_at " + sort).Find(&itens).Error
	}
	return itens, err
}

func (r *CursoRepositoryGorm) HasAlunoPagamentoPendente(alunoID uuid.UUID) (bool, error) {
	var count int64
	err := r.DB.Model(&entity.AlunoCurso{}).Where("aluno_id = ? AND status_pagamento = ?", alunoID.String(), entity.PagamentoPendente).Count(&count).Error
	if err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	return false, nil
}

// endregion

// region CRUD AlunoCurso
func (r *CursoRepositoryGorm) CreateAlunoCurso(obj *entity.AlunoCurso) (*entity.AlunoCurso, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}
func (r *CursoRepositoryGorm) UpdateAlunoCurso(obj *entity.AlunoCurso) (*entity.AlunoCurso, error) {
	s, err := r.GetAlunoCurso(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}
func (r *CursoRepositoryGorm) DeleteAlunoCurso(objID uuid.UUID) error {
	obj, err := r.GetAlunoCurso(objID)
	if err != nil {
		return err
	}
	tx := r.DB.Begin()

	// deleta os filhos primeiro
	if err := tx.Where("aluno_curso_id = ?", obj.ID).Delete(&entity.AlunoCursoItemModulo{}).Error; err != nil {
		tx.Rollback()
		return err
	}

	// depois deleta o pai
	if err := tx.Delete(&entity.AlunoCurso{}, obj.ID).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}
func (r *CursoRepositoryGorm) GetAlunoCurso(objID uuid.UUID) (*entity.AlunoCurso, error) {
	var obj entity.AlunoCurso
	err := r.DB.Preload("Aluno").Preload("Curso").Where("id = ?", objID.String()).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}
func (r *CursoRepositoryGorm) FindAllAlunoCursos(page, limit int, sort string) ([]entity.AlunoCurso, error) {
	var itens []entity.AlunoCurso
	var err error
	if sort != "" && sort != "asc" && sort != "desc" {
		sort = "asc"
	}
	preloads := r.DB.Preload("Aluno").Preload("Curso")
	if page != 0 && limit != 0 {
		err = preloads.Limit(limit).Offset((page - 1) * limit).Order("created_at " + sort).Find(&itens).Error
	} else {
		err = preloads.Order("created_at " + sort).Find(&itens).Error
	}
	return itens, err
}
func (r *CursoRepositoryGorm) FindCursosDoAluno(alunoID uuid.UUID) ([]entity.AlunoCurso, error) {
	var itens []entity.AlunoCurso
	err := r.DB.Preload("Aluno").Preload("Curso").Where("aluno_id = ?", alunoID.String()).Find(&itens).Error
	if err != nil {
		return nil, err
	}
	return itens, nil
}
func (r *CursoRepositoryGorm) FindAlunosDoCurso(cursoID uuid.UUID) ([]entity.AlunoCurso, error) {
	var itens []entity.AlunoCurso
	err := r.DB.Preload("Aluno").Preload("Curso").Where("curso_id = ?", cursoID.String()).Find(&itens).Error
	if err != nil {
		return nil, err
	}
	return itens, nil
}
func (r *CursoRepositoryGorm) CountCursosDoAluno(alunoID uuid.UUID) (int64, error) {
	var count int64
	err := r.DB.Model(&entity.AlunoCurso{}).Where("aluno_id = ?", alunoID.String()).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}
func (r *CursoRepositoryGorm) CountAlunosDoCurso(cursoID uuid.UUID) (int64, error) {
	var count int64
	err := r.DB.Model(&entity.AlunoCurso{}).Where("curso_id = ?", cursoID.String()).Count(&count).Error
	if err != nil {
		return 0, err
	}
	return count, nil
}

// endregion

// region CRUD ItemModulo
func (r *CursoRepositoryGorm) CreateItemModulo(item *entity.ItemModulo) error {
	if err := r.DB.Create(item).Error; err != nil {
		return err
	}
	return nil
}

func (r *CursoRepositoryGorm) FindItemModuloByID(id uuid.UUID) (*entity.ItemModulo, error) {
	var item entity.ItemModulo
	err := r.DB.
		Preload("Aula").
		Preload("ContractValidation").
		Preload("Video").
		Where("id = ?", id.String()).First(&item).Error
	if err != nil {
		return nil, err
	}
	return &item, nil
}
func (r *CursoRepositoryGorm) FindItemModulosByModulo(moduloID uuid.UUID) ([]entity.ItemModulo, error) {
	var itens []entity.ItemModulo
	err := r.DB.
		Preload("Aula").
		Preload("ContractValidation").
		Preload("Video").
		Where("modulo_id = ?", moduloID.String()).Find(&itens).Error
	if err != nil {
		return nil, err
	}
	return itens, nil
}
func (r *CursoRepositoryGorm) UpdateItemModulo(item *entity.ItemModulo) error {
	s, err := r.FindItemModuloByID(item.ID)
	if err != nil {
		return err
	}
	item.CreatedAt = s.CreatedAt
	if err := r.DB.Save(item).Error; err != nil {
		return err
	}
	return nil
}
func (r *CursoRepositoryGorm) DeleteItemModulo(id uuid.UUID) error {
	item, err := r.FindItemModuloByID(id)
	if err != nil {
		return err
	}
	return r.DB.Delete(&entity.ItemModulo{}, item.ID).Error
}
func (r *CursoRepositoryGorm) MoveItemModulo(id uuid.UUID, action string) error {
	var item entity.ItemModulo
	if err := r.DB.First(&item, "id = ?", id).Error; err != nil {
		return err
	}

	var vizinho entity.ItemModulo
	ordemAtual := item.Ordem

	switch action {
	case "cima":
		if err := r.DB.Where("modulo_id = ? AND ordem < ?", item.ModuloID, ordemAtual).
			Order("ordem desc").First(&vizinho).Error; err != nil {
			return err
		}
	case "baixo":
		if err := r.DB.Where("modulo_id = ? AND ordem > ?", item.ModuloID, ordemAtual).
			Order("ordem asc").First(&vizinho).Error; err != nil {
			return err
		}
	case "inicio":
		if err := r.DB.Model(&entity.ItemModulo{}).
			Where("modulo_id = ? AND ordem < ?", item.ModuloID, ordemAtual).
			Update("ordem", gorm.Expr("ordem + 1")).Error; err != nil {
			return err
		}
		item.Ordem = 1
		return r.DB.Save(&item).Error
	case "fim":
		var max int
		if err := r.DB.Model(&entity.ItemModulo{}).
			Where("modulo_id = ?", item.ModuloID).
			Select("COALESCE(MAX(ordem), 0)").Scan(&max).Error; err != nil {
			return err
		}
		item.Ordem = max + 1
		return r.DB.Save(&item).Error
	default:
		return errors.New("ação inválida")
	}

	temp := item.Ordem
	item.Ordem = vizinho.Ordem
	vizinho.Ordem = temp

	if err := r.DB.Save(&item).Error; err != nil {
		return err
	}
	return r.DB.Save(&vizinho).Error
}

func (r *CursoRepositoryGorm) GetMaxOrdemItemModulo(moduloID uuid.UUID) (int, error) {
	var maxOrdem int
	err := r.DB.Model(&entity.ItemModulo{}).
		Where("modulo_id = ?", moduloID.String()).
		Select("COALESCE(MAX(ordem), 0)").Scan(&maxOrdem).Error
	if err != nil {
		return 0, err
	}
	return maxOrdem, nil
}

// endregion

// region métodos AlunoCursoItemModulo

// CreateAlunoCursoItemModulosBatch cria todos os registros de itens da matrícula de uma vez.
func (r *CursoRepositoryGorm) CreateAlunoCursoItemModulosBatch(items []*entity.AlunoCursoItemModulo) error {
	if len(items) == 0 {
		return nil
	}
	return r.DB.Create(&items).Error
}

// FindItemModulosByAlunoCurso lista todos os itens de módulo de uma matrícula.
func (r *CursoRepositoryGorm) FindItemModulosByAlunoCurso(alunoCursoID uuid.UUID) ([]entity.AlunoCursoItemModulo, error) {
	var itens []entity.AlunoCursoItemModulo
	err := r.DB.
		Preload("ItemModulo").
		Preload("ItemModulo.Aula").
		Preload("ItemModulo.ContractValidation").
		Preload("ItemModulo.Video").
		Preload("AlunoCurso").
		Where("aluno_curso_id = ?", alunoCursoID).Find(&itens).Error
	return itens, err
}

// GetAlunoCursoItemModulo busca um item específico de uma matrícula.
func (r *CursoRepositoryGorm) GetAlunoCursoItemModulo(id uuid.UUID) (*entity.AlunoCursoItemModulo, error) {
	var item entity.AlunoCursoItemModulo
	err := r.DB.Preload("ItemModulo").Preload("AlunoCurso").First(&item, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// UpdateAlunoCursoItemModulo atualiza status, progresso ou campos específicos do item.
func (r *CursoRepositoryGorm) UpdateAlunoCursoItemModulo(item *entity.AlunoCursoItemModulo) error {
	return r.DB.Model(&entity.AlunoCursoItemModulo{}).
		Where("id = ?", item.ID).
		Updates(item).Error
}

// endregion
