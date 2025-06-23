package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewModulo_ErrorIfEmptyID(t *testing.T) {
	obj := Modulo{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewModulo_ErrorIfEmptyNome(t *testing.T) {
	_, err := NewModulo(uuid.New(), nil, "", "descricao")
	assert.Error(t, err, "invalid nome")
}

func TestNewModulo_ErrorIfEmptyDesCricao(t *testing.T) {
	_, err := NewModulo(uuid.New(), nil, "nome", "")
	assert.Error(t, err, "invalid descricao")
}

func TestNewModulo_Success(t *testing.T) {
	obj, err := NewModulo(uuid.New(), nil, "nome", "descricao")
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, "nome", obj.Nome)
	assert.Equal(t, "descricao", obj.Descricao)
}
