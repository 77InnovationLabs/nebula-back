package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Testes para Entidade Curso
func TestNewCurso_ErrorIfEmptyID(t *testing.T) {
	obj := Curso{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewCurso_ErrorIfEmptyName(t *testing.T) {
	obj := Curso{
		ID:        uuid.New(),
		Descricao: "descricao",
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid nome", err.Error())
}

func TestNewCurso_ErrorIfEmptyDescricao(t *testing.T) {
	obj := Curso{
		ID:   uuid.New(),
		Nome: "nome",
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid descricao", err.Error())
}

func TestNewCurso_Success(t *testing.T) {
	t.Run("sucesso se tudo é informado", func(t *testing.T) {

		obj := Curso{
			ID:        uuid.New(),
			Nome:      "nome",
			Descricao: "descricao",
		}
		assert.Nil(t, obj.IsValid())
	})

}
