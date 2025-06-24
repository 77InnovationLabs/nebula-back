package gorm

import (
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/entity"
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/repository"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Verifica se essa IMPLEMENTAÇÃO implementa corretamente a INTERFACE
var _ repository.PessoaRepositoryInterface = &PessoaRepositoryGorm{}

type PessoaRepositoryGorm struct {
	DB *gorm.DB
}

func NewPessoaRepositoryGorm(db *gorm.DB) *PessoaRepositoryGorm {
	return &PessoaRepositoryGorm{DB: db}
}

func (r *PessoaRepositoryGorm) CreatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) GetPessoa(objID uuid.UUID) (*entity.Pessoa, error) {
	var obj entity.Pessoa
	err := r.DB.Where("id = ?", objID).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *PessoaRepositoryGorm) UpdatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error) {
	s, err := r.GetPessoa(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) DeletePessoa(objID uuid.UUID) error {
	return r.DB.Delete(&entity.Pessoa{}, objID).Error
}

func (r *PessoaRepositoryGorm) GetPessoas() ([]entity.Pessoa, error) {
	var itens []entity.Pessoa
	err := r.DB.Find(&itens).Error
	return itens, err
}
