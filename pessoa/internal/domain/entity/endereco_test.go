package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewEndereco_ErrorIfEmptyID(t *testing.T) {
	obj := Endereco{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewEndereco_ErrorIfEmptyLogradouro(t *testing.T) {
	_, err := NewEndereco(uuid.New(), nil, "", "123", "Bairro", "Cidade", "UF", "12345678", false, false)
	assert.Error(t, err, "invalid logradouro")
}

func TestNewEndereco_ErrorIfEmptyCEP(t *testing.T) {
	_, err := NewEndereco(uuid.New(), nil, "Logradouro", "123", "Bairro", "Cidade", "UF", "", false, false)
	assert.Error(t, err, "invalid cep")
}

func TestNewEndereco_ErrorIfEmptyBairro(t *testing.T) {
	_, err := NewEndereco(uuid.New(), nil, "Logradouro", "123", "", "Cidade", "UF", "12345678", false, false)
	assert.Error(t, err, "invalid bairro")
}

func TestNewEndereco_ErrorIfEmptyCidade(t *testing.T) {
	_, err := NewEndereco(uuid.New(), nil, "Logradouro", "123", "Bairro", "", "UF", "12345678", false, false)
	assert.Error(t, err, "invalid cidade")
}

func TestNewEndereco_ErrorIfEmptyEstado(t *testing.T) {
	_, err := NewEndereco(uuid.New(), nil, "Logradouro", "123", "Bairro", "Cidade", "", "12345678", false, false)
	assert.Error(t, err, "invalid estado")
}

func TestNewEndereco_Success(t *testing.T) {
	obj, err := NewEndereco(uuid.New(), nil, "Logradouro", "123", "12345678", "Bairro", "Cidade", "UF", false, false)
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, "Logradouro", obj.Logradouro)
	assert.Equal(t, "123", obj.Numero)
	assert.Equal(t, "Bairro", obj.Bairro)
	assert.Equal(t, "Cidade", obj.Cidade)
	assert.Equal(t, "UF", obj.Estado)
	assert.Equal(t, "12345678", obj.CEP)
	assert.False(t, obj.Principal)
	assert.False(t, obj.SemNumero)
}
