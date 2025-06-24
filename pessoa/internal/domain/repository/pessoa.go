package repository

import (
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	"github.com/google/uuid"
)

type PessoaRepositoryInterface interface {
	CreatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error)
	UpdatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error)
	DeletePessoa(objID uuid.UUID) error
	GetPessoa(objID uuid.UUID) (*entity.Pessoa, error)
	GetPessoaByDocumento(documento string) (*entity.Pessoa, error)
	FindAllPessoas(page, limit int, sort string) ([]entity.Pessoa, error)

	CreateEndereco(obj *entity.Endereco) (*entity.Endereco, error)
	UpdateEndereco(obj *entity.Endereco) (*entity.Endereco, error)
	DeleteEndereco(objID uuid.UUID) error
	GetEndereco(objID uuid.UUID) (*entity.Endereco, error)
	GetEnderecosDaPessoa(parentID uuid.UUID) ([]entity.Endereco, error)

	CreateTelefone(obj *entity.Telefone) (*entity.Telefone, error)
	UpdateTelefone(obj *entity.Telefone) (*entity.Telefone, error)
	DeleteTelefone(objID uuid.UUID) error
	GetTelefone(objID uuid.UUID) (*entity.Telefone, error)
	GetTelefonesDaPessoa(parentID uuid.UUID) ([]entity.Telefone, error)

	CreateEmail(obj *entity.Email) (*entity.Email, error)
	UpdateEmail(obj *entity.Email) (*entity.Email, error)
	DeleteEmail(objID uuid.UUID) error
	GetEmail(objID uuid.UUID) (*entity.Email, error)
	GetEmailsDaPessoa(parentID uuid.UUID) ([]entity.Email, error)
}
