package gorm

import (
	"fmt"
	"testing"

	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateNewPessoa(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret.ID)
}

func TestGetPessoaByID(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
}

func TestGetPessoaByID_NotFound(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret, err := pessoaDB.GetPessoa(uuid.New())
	assert.Error(t, err)
	assert.Nil(t, ret)
}

func TestGetPessoaByID_comEnderecos(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	endereco1, err := entity.NewEndereco(pessoa.ID, nil, "Rua 1", "123", "08889888", "Bairro 1", "Cidade 1", "UF 1", true, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco1)

	endereco2, err := entity.NewEndereco(pessoa.ID, nil, "Rua 2", "654", "08889012", "Bairro 2", "Cidade 2", "SP", false, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco2)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
	assert.Equal(t, len(ret1.Enderecos), len(ret2.Enderecos))
	assert.Equal(t, len(ret1.Enderecos), 2)
}

func TestGetPessoaByID_comEnderecosTelefonesEmails(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	endereco1, err := entity.NewEndereco(pessoa.ID, nil, "Rua 1", "123", "08889888", "Bairro 1", "Cidade 1", "UF 1", true, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco1)

	endereco2, err := entity.NewEndereco(pessoa.ID, nil, "Rua 2", "654", "08889012", "Bairro 2", "Cidade 2", "SP", false, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco2)

	email1, err := entity.NewEmail(pessoa.ID, nil, "hgfh@jkjh.com", true)
	assert.NoError(t, err)
	pessoa.Emails = append(pessoa.Emails, *email1)

	email2, err := entity.NewEmail(pessoa.ID, nil, "utusy@eme.com", false)
	assert.NoError(t, err)
	pessoa.Emails = append(pessoa.Emails, *email2)

	telefone1, err := entity.NewTelefone(pessoa.ID, nil, "11", "123456789", true)
	assert.NoError(t, err)
	pessoa.Telefones = append(pessoa.Telefones, *telefone1)

	telefone2, err := entity.NewTelefone(pessoa.ID, nil, "11", "987654321", false)
	assert.NoError(t, err)
	pessoa.Telefones = append(pessoa.Telefones, *telefone2)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
	assert.Equal(t, len(ret1.Enderecos), len(ret2.Enderecos))
	assert.Equal(t, len(ret1.Enderecos), 2)
	assert.Equal(t, len(ret1.Emails), len(ret2.Emails))
	assert.Equal(t, len(ret1.Emails), 2)
	assert.Equal(t, len(ret1.Telefones), len(ret2.Telefones))
	assert.Equal(t, len(ret1.Telefones), 2)
}

func TestGetPessoaByID_comEnderecosTelefonesEmails_ErrorIfOneNotformattedEmail(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	endereco1, err := entity.NewEndereco(pessoa.ID, nil, "Rua 1", "123", "08889888", "Bairro 1", "Cidade 1", "UF 1", true, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco1)

	endereco2, err := entity.NewEndereco(pessoa.ID, nil, "Rua 2", "654", "08889012", "Bairro 2", "Cidade 2", "SP", false, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco2)

	email1, err := entity.NewEmail(pessoa.ID, nil, "jjjjjj", true)
	assert.Error(t, err)
	assert.Nil(t, email1)
	//assert error message
	assert.Equal(t, "invalid email", err.Error())

	email2, err := entity.NewEmail(pessoa.ID, nil, "jhgjhg@ok.com", false)
	assert.NoError(t, err)
	pessoa.Emails = append(pessoa.Emails, *email2)

	telefone1, err := entity.NewTelefone(pessoa.ID, nil, "11", "123456789", true)
	assert.NoError(t, err)
	pessoa.Telefones = append(pessoa.Telefones, *telefone1)

	telefone2, err := entity.NewTelefone(pessoa.ID, nil, "11", "987654321", false)
	assert.NoError(t, err)
	pessoa.Telefones = append(pessoa.Telefones, *telefone2)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
	assert.Equal(t, len(ret1.Enderecos), len(ret2.Enderecos))
	assert.Equal(t, len(ret1.Enderecos), 2)
	assert.Equal(t, len(ret1.Emails), len(ret2.Emails))
	assert.Equal(t, len(ret1.Emails), 1)
	assert.Equal(t, len(ret1.Telefones), len(ret2.Telefones))
	assert.Equal(t, len(ret1.Telefones), 2)
}

func TestGetPessoaByID_comEnderecosTelefonesEmails_ErrorIfOneNotformattedTelefone(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	endereco1, err := entity.NewEndereco(pessoa.ID, nil, "Rua 1", "123", "08889888", "Bairro 1", "Cidade 1", "UF 1", true, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco1)

	endereco2, err := entity.NewEndereco(pessoa.ID, nil, "Rua 2", "654", "08889012", "Bairro 2", "Cidade 2", "SP", false, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco2)

	email1, err := entity.NewEmail(pessoa.ID, nil, "jjjjjj", true)
	assert.Error(t, err)
	assert.Nil(t, email1)
	//assert error message
	assert.Equal(t, "invalid email", err.Error())

	email2, err := entity.NewEmail(pessoa.ID, nil, "lkjlkj@copioi.com", false)
	assert.NoError(t, err)
	pessoa.Emails = append(pessoa.Emails, *email2)

	telefone1, err := entity.NewTelefone(pessoa.ID, nil, "11", "123456789", true)
	assert.NoError(t, err)
	pessoa.Telefones = append(pessoa.Telefones, *telefone1)

	telefone2, err := entity.NewTelefone(pessoa.ID, nil, "dd", "987654321", false)
	assert.Error(t, err)
	assert.Nil(t, telefone2)
	//assert error message
	assert.Equal(t, "invalid ddd", err.Error())

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
	assert.Equal(t, len(ret1.Enderecos), len(ret2.Enderecos))
	assert.Equal(t, len(ret1.Enderecos), 2)
	assert.Equal(t, len(ret1.Emails), len(ret2.Emails))
	assert.Equal(t, len(ret1.Emails), 1)
	assert.Equal(t, len(ret1.Telefones), len(ret2.Telefones))
	assert.Equal(t, len(ret1.Telefones), 1)
}

func TestGetByDocumento(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoaByDocumento("cpf da pessoas")
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
}

func TestDelete(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)

	err = pessoaDB.DeletePessoa(ret1.ID)
	assert.NoError(t, err)

	ret3, err := pessoaDB.GetPessoa(ret1.ID)
	assert.Error(t, err)
	assert.Nil(t, ret3)
}

func TestDelete_comEndereco_verificaSeSenderecoFoiDeletado(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	pessoa, err := entity.NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoas")
	assert.NoError(t, err)

	endereco1, err := entity.NewEndereco(pessoa.ID, nil, "Rua 1", "123", "08889888", "Bairro 1", "Cidade 1", "UF 1", true, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco1)

	endereco2, err := entity.NewEndereco(pessoa.ID, nil, "Rua 2", "654", "08889012", "Bairro 2", "Cidade 2", "SP", false, false)
	assert.NoError(t, err)
	pessoa.Enderecos = append(pessoa.Enderecos, *endereco2)

	pessoaDB := NewPessoaRepositoryGorm(db)
	ret1, err := pessoaDB.CreatePessoa(pessoa)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret1.ID)

	ret2, err := pessoaDB.GetPessoa(ret1.ID)
	assert.NoError(t, err)
	assert.NotEmpty(t, ret2.ID)
	assert.Equal(t, ret1.ID, ret2.ID)
	assert.Equal(t, ret1.Tipo, ret2.Tipo)
	assert.Equal(t, ret1.Nome, ret2.Nome)
	assert.Equal(t, ret1.Documento, ret2.Documento)
	assert.Equal(t, len(ret1.Enderecos), len(ret2.Enderecos))
	assert.Equal(t, len(ret1.Enderecos), 2)

	err = pessoaDB.DeletePessoa(ret1.ID)
	assert.NoError(t, err)

	ret3, err := pessoaDB.GetPessoa(ret1.ID)
	assert.Error(t, err)
	assert.Nil(t, ret3)
}

func TestFinalAllPessoas(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Email{}, &entity.Telefone{}, &entity.Endereco{})

	for i := 1; i < 24; i++ {
		item, err := entity.NewPessoa(nil, "FISICA", fmt.Sprintf("Pessoa %d", i), fmt.Sprintf("cpf da pessoa %d", i))
		assert.NoError(t, err)
		db.Save(item)
	}

	repo := NewPessoaRepositoryGorm(db)
	itens, err := repo.FindAllPessoas(1, 10, "asc")
	assert.NoError(t, err)
	assert.Len(t, itens, 10)
	assert.Equal(t, "Pessoa 1", itens[0].Nome)
	assert.Equal(t, "Pessoa 10", itens[9].Nome)

	itens, err = repo.FindAllPessoas(2, 10, "asc")
	assert.NoError(t, err)
	assert.Len(t, itens, 10)
	assert.Equal(t, "Pessoa 11", itens[0].Nome)
	assert.Equal(t, "Pessoa 20", itens[9].Nome)

	itens, err = repo.FindAllPessoas(3, 10, "asc")
	assert.NoError(t, err)
	assert.Len(t, itens, 3)
	assert.Equal(t, "Pessoa 21", itens[0].Nome)
	assert.Equal(t, "Pessoa 23", itens[2].Nome)
}
