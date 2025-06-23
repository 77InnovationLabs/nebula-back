package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewTelefone_ErrorIfEmptyID(t *testing.T) {
	obj := Telefone{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewTelefone_ErrorIfEmptyDDD(t *testing.T) {
	_, err := NewTelefone(uuid.New(), nil, "", "123456789", false)
	assert.Error(t, err, "invalid ddd")
}

func TestNewTelefone_ErrorIfEmptyNumero(t *testing.T) {
	_, err := NewTelefone(uuid.New(), nil, "11", "", false)
	assert.Error(t, err, "invalid numero")
}

func TestNewTelefone_ErrorIfDDDisNotNumber(t *testing.T) {
	_, err := NewTelefone(uuid.New(), nil, "abc", "123456789", false)
	assert.Error(t, err, "invalid ddd")
}

func TestNewTelefone_ErrorIfNumeroIsNotNumber(t *testing.T) {
	_, err := NewTelefone(uuid.New(), nil, "11", "abc", false)
	assert.Error(t, err, "invalid numero")
}

func TestNewTelefone_Success(t *testing.T) {
	obj, err := NewTelefone(uuid.New(), nil, "11", "123456789", false)
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, "11", obj.DDD)
	assert.Equal(t, "123456789", obj.Numero)
	assert.False(t, obj.Principal)
}
