package gorm

import (
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/entity"
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/repository"
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

func (r *PessoaRepositoryGorm) CreateEndereco(obj *entity.Endereco) (*entity.Endereco, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) GetEndereco(objID uuid.UUID) (*entity.Endereco, error) {
	var obj entity.Endereco
	err := r.DB.Where("id = ?", objID).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *PessoaRepositoryGorm) UpdateEndereco(obj *entity.Endereco) (*entity.Endereco, error) {
	s, err := r.GetEndereco(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) DeleteEndereco(objID uuid.UUID) error {
	return r.DB.Delete(&entity.Endereco{}, objID).Error
}

func (r *PessoaRepositoryGorm) GetEnderecosDaPessoa(parentID uuid.UUID) ([]entity.Endereco, error) {
	var itens []entity.Endereco
	err := r.DB.Where("pessoa_id = ?", parentID).Find(&itens).Error
	return itens, err
}

func (r *PessoaRepositoryGorm) CreateTelefone(obj *entity.Telefone) (*entity.Telefone, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) GetTelefone(objID uuid.UUID) (*entity.Telefone, error) {
	var obj entity.Telefone
	err := r.DB.Where("id = ?", objID).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *PessoaRepositoryGorm) UpdateTelefone(obj *entity.Telefone) (*entity.Telefone, error) {
	s, err := r.GetTelefone(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) DeleteTelefone(objID uuid.UUID) error {
	return r.DB.Delete(&entity.Telefone{}, objID).Error
}

func (r *PessoaRepositoryGorm) GetTelefonesDaPessoa(parentID uuid.UUID) ([]entity.Telefone, error) {
	var itens []entity.Telefone
	err := r.DB.Where("pessoa_id = ?", parentID).Find(&itens).Error
	return itens, err
}

func (r *PessoaRepositoryGorm) CreateEmail(obj *entity.Email) (*entity.Email, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) GetEmail(objID uuid.UUID) (*entity.Email, error) {
	var obj entity.Email
	err := r.DB.Where("id = ?", objID).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *PessoaRepositoryGorm) UpdateEmail(obj *entity.Email) (*entity.Email, error) {
	s, err := r.GetEmail(obj.ID)
	if err != nil {
		return nil, err
	}
	obj.CreatedAt = s.CreatedAt
	if err := r.DB.Save(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *PessoaRepositoryGorm) DeleteEmail(objID uuid.UUID) error {
	return r.DB.Delete(&entity.Email{}, objID).Error
}

func (r *PessoaRepositoryGorm) GetEmailsDaPessoa(parentID uuid.UUID) ([]entity.Email, error) {
	var itens []entity.Email
	err := r.DB.Where("pessoa_id = ?", parentID).Find(&itens).Error
	return itens, err
}

func (r *PessoaRepositoryGorm) CreatePessoa(obj *entity.Pessoa) (*entity.Pessoa, error) {
	if err := r.DB.Create(obj).Error; err != nil {
		return nil, err
	}
	return obj, nil
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

func (r *PessoaRepositoryGorm) GetPessoa(objID uuid.UUID) (*entity.Pessoa, error) {
	var obj entity.Pessoa
	err := r.DB.Preload("Enderecos").Preload("Telefones").Preload("Emails").Where("id = ?", objID.String()).First(&obj).Error
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (r *PessoaRepositoryGorm) GetPessoaByDocumento(documento string) (*entity.Pessoa, error) {
	var pessoa entity.Pessoa
	err := r.DB.Where("documento = ?", documento).First(&pessoa).Error
	if err != nil {
		return nil, err
	}
	return &pessoa, nil
}

func (r *PessoaRepositoryGorm) FindAllPessoas(page, limit int, sort string) ([]entity.Pessoa, error) {
	var itens []entity.Pessoa
	var err error
	if sort != "" && sort != "asc" && sort != "desc" {
		sort = "asc"
	}
	preloads := r.DB.Preload("Enderecos").Preload("Telefones").Preload("Emails")
	if page != 0 && limit != 0 {
		err = preloads.Limit(limit).Offset((page - 1) * limit).Order("created_at " + sort).Find(&itens).Error
	} else {
		err = preloads.Order("created_at " + sort).Find(&itens).Error
	}
	return itens, err
}
