package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Testes para Entidade Pessoa
func TestNewPessoa(t *testing.T) {
	// Cenário 1: Dados válidos
	id := uuid.New()
	pessoa, err := NewPessoa(&id, "FISICA", "João")
	assert.Nil(t, err)
	assert.NotNil(t, pessoa)
	assert.Equal(t, TipoPessoa("FISICA"), pessoa.Tipo)
	assert.Equal(t, "João", pessoa.Nome)

	// Cenário 2: Dados inválidos
	pessoa, err = NewPessoa(&uuid.Nil, "", "")
	assert.NotNil(t, err)
	assert.Nil(t, pessoa)
}

func TestIsValid(t *testing.T) {
	// Cenário 1: Dados válidos
	id := uuid.New()
	pessoa := Pessoa{
		ID:   id,
		Tipo: "FISICA",
		Nome: "João",
	}
	err := pessoa.IsValid()
	assert.Nil(t, err)

	// Cenário 2: ID inválido
	pessoa.ID = uuid.Nil
	err = pessoa.IsValid()
	assert.NotNil(t, err)

	// Cenário 3: Tipo inválido
	pessoa.ID = id
	pessoa.Tipo = ""
	err = pessoa.IsValid()
	assert.NotNil(t, err)

	// Cenário 4: Tipo inválido
	pessoa.Tipo = "JURIDICA"
	err = pessoa.IsValid()
	assert.Nil(t, err)

	// Cenário 5: Nome inválido
	pessoa.Nome = ""
	err = pessoa.IsValid()
	assert.NotNil(t, err)
}
