package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Testes para Entidade Pessoa
func TestNewPessoa_ErrorIfEmptyID(t *testing.T) {
	obj := Pessoa{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewPessoa_ErrorIfEmptyTipo(t *testing.T) {
	_, err := NewPessoa(nil, "", "Nome Da Pessoa", "cpf da pessoa")
	assert.Error(t, err, "invalid tipo")
}

func TestNewPessoa_ErrorIfWrongTipo(t *testing.T) {
	_, err := NewPessoa(nil, "xxx", "Nome Da Pessoa", "cpf da pessoa")
	assert.Error(t, err, "invalid tipo")
}

func TestNewPessoa_ErrorIfEmptyNome(t *testing.T) {
	_, err := NewPessoa(nil, "FISICA", "", "cpf da pessoa")
	assert.Error(t, err, "invalid name")

	_, err = NewPessoa(nil, "JURIDICA", "", "cpf da pessoa")
	assert.Error(t, err, "invalid name")
}

func TestNewPessoa_ErrorIfEmptyDocumento(t *testing.T) {
	_, err := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "")
	assert.Error(t, err, "invalid document")

	_, err = NewPessoa(nil, "JURIDICA", "Nome Da Pessoa", "")
	assert.Error(t, err, "invalid document")
}

func TestNewPessoa_Fisica_Success(t *testing.T) {
	obj, err := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, TipoPessoa("FISICA"), obj.Tipo)
	assert.Equal(t, "Nome Da Pessoa", obj.Nome)
	assert.Equal(t, "cpf da pessoa", obj.Documento)
	assert.NotEqual(t, uuid.Nil, obj.ID)
	assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", obj.ID.String())
}

func TestNewPessoa_Fisica_WithID_Success(t *testing.T) {
	id := uuid.New()
	obj, err := NewPessoa(&id, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.Equal(t, id, obj.ID)
	assert.Equal(t, TipoPessoa("FISICA"), obj.Tipo)
	assert.Equal(t, "Nome Da Pessoa", obj.Nome)
	assert.Equal(t, "cpf da pessoa", obj.Documento)
}

func TestNewPessoa_Fisica_WithUuidNil_Success(t *testing.T) {
	id := uuid.Nil
	obj, err := NewPessoa(&id, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", obj.ID.String())
	assert.Equal(t, TipoPessoa("FISICA"), obj.Tipo)
	assert.Equal(t, "Nome Da Pessoa", obj.Nome)
	assert.Equal(t, "cpf da pessoa", obj.Documento)
}

func TestNewPessoa_Juridica_Success(t *testing.T) {
	obj, err := NewPessoa(nil, "JURIDICA", "Nome Da Empresa", "cnpj da empresa")
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, TipoPessoa("JURIDICA"), obj.Tipo)
	assert.Equal(t, "Nome Da Empresa", obj.Nome)
	assert.Equal(t, "cnpj da empresa", obj.Documento)
}

func TestNewPessoa_IsValidDeep_ErrorIfInvalidEmail(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	email := Email{
		ID:        uuid.New(),
		Endereco:  "invalid-email",
		Principal: false,
		PessoaID:  obj.ID,
	}
	obj.Emails = append(obj.Emails, email)
	assert.Error(t, obj.IsValidDeep(), "invalid email")
}

func TestNewPessoa_IsValidDeep_ErrorIfEmptyEmail(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	email := Email{
		ID:        uuid.New(),
		Endereco:  "",
		Principal: false,
		PessoaID:  obj.ID,
	}
	obj.Emails = append(obj.Emails, email)
	assert.Error(t, obj.IsValidDeep())
	assert.Equal(t, string(obj.IsValidDeep().Error()), "invalid email")
}

func TestNewPessoa_IsValidDeep_ErrorIfInvalidDdd(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	telefone := Telefone{
		ID:        uuid.New(),
		DDD:       "xx",
		Numero:    "123456789",
		Principal: false,
		PessoaID:  obj.ID,
	}
	obj.Telefones = append(obj.Telefones, telefone)
	assert.Error(t, obj.IsValidDeep())
	assert.Equal(t, string(obj.IsValidDeep().Error()), "invalid ddd")
}

func TestNewPessoa_IsValidDeep_ErrorIfInvalidTelefone(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	telefone := Telefone{
		ID:        uuid.New(),
		DDD:       "11",
		Numero:    "invalid-number",
		Principal: false,
		PessoaID:  obj.ID,
	}
	obj.Telefones = append(obj.Telefones, telefone)
	assert.Error(t, obj.IsValidDeep())
	assert.Equal(t, string(obj.IsValidDeep().Error()), "invalid numero")
}

func TestNewPessoa_IsValidDeep_ErrorIfInvalidEndereco(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	endereco := Endereco{
		ID:         uuid.New(),
		Logradouro: "",
		Numero:     "123",
		Bairro:     "O bairro",
		Cidade:     "cidade",
		Estado:     "SP",
		CEP:        "55121512",
		Principal:  false,
		SemNumero:  false,
		PessoaID:   obj.ID,
	}
	obj.Enderecos = append(obj.Enderecos, endereco)
	assert.Error(t, obj.IsValidDeep())
	assert.Equal(t, string(obj.IsValidDeep().Error()), "invalid logradouro")
}

func TestNewPessoa_IsValidDeep_Success(t *testing.T) {
	obj, _ := NewPessoa(nil, "FISICA", "Nome Da Pessoa", "cpf da pessoa")
	email, _ := NewEmail(obj.ID, nil, "xpto@email.com", false)
	obj.Emails = append(obj.Emails, *email)

	telefone, _ := NewTelefone(obj.ID, nil, "11", "123456789", false)
	obj.Telefones = append(obj.Telefones, *telefone)

	outro_telefone, _ := NewTelefone(obj.ID, nil, "11", "987654321", false)
	obj.Telefones = append(obj.Telefones, *outro_telefone)

	endereco := Endereco{
		ID:         uuid.New(),
		Logradouro: "Rua XPTO",
		Numero:     "123",
		Bairro:     "Bairro",
		Cidade:     "Cidade",
		Estado:     "UF",
		CEP:        "12345678",
		Principal:  false,
		SemNumero:  false,
		PessoaID:   obj.ID,
	}
	obj.Enderecos = append(obj.Enderecos, endereco)

	assert.Nil(t, obj.IsValidDeep())
}
