package repository

import (
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"
	"github.com/google/uuid"
)

type PessoaRepositoryInterface interface {
	CreatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error)
	UpdatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error)
	DeletePessoa(objID uuid.UUID) error
	GetPessoa(objID uuid.UUID) (*entity.Pessoa, error)
	GetPessoas() ([]entity.Pessoa, error)
}
