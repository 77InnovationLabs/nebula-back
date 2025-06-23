package entity

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Testes para Entidade Aluno
func TestNewAluno_ErrorIfEmptyID(t *testing.T) {
	obj := Aluno{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewAluno_ErrorIfEmptyPessoaID(t *testing.T) {
	obj := Aluno{
		ID:          uuid.New(),
		StatusAluno: StatusAluno("INATIVO"),
		Wallet:      "0x00anything",
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid pessoa", err.Error())
}
func TestNewAluno_ErrorIfEmptyWallet(t *testing.T) {
	dataInicio := time.Now()
	obj := Aluno{
		ID:          uuid.New(),
		PessoaID:    uuid.New(),
		StatusAluno: StatusAluno("ATIVO"),
		DataInicio:  &dataInicio,
		// Wallet:      "0x00anything",
		NftId: "0x00anything",
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid wallet", err.Error())
}

func TestNewAluno_ErrorIfWrongTipo(t *testing.T) {
	t.Run("erro se status não informado", func(t *testing.T) {
		obj := Aluno{
			ID:       uuid.New(),
			PessoaID: uuid.New(),
			Wallet:   "0x00anything",
			NftId:    "0x00anything",
		}
		err := obj.IsValid()
		assert.Error(t, err)
		assert.Equal(t, "invalid status", err.Error())
	})

	t.Run("erro se status diferente de ATIVO ou INATIVO", func(t *testing.T) {
		obj := Aluno{
			ID:          uuid.New(),
			PessoaID:    uuid.New(),
			StatusAluno: StatusAluno("INVALIDO"),
			Wallet:      "0x00anything",
			NftId:       "0x00anything",
		}
		err := obj.IsValid()
		assert.Error(t, err)
		assert.Equal(t, "invalid status", err.Error())
	})
}

func TestNewAluno_Success(t *testing.T) {
	t.Run("sucesso se data de inicio é informada", func(t *testing.T) {

		dataInicio := time.Now()

		obj := Aluno{
			ID:          uuid.New(),
			PessoaID:    uuid.New(),
			StatusAluno: StatusAluno("ATIVO"),
			DataInicio:  &dataInicio,
			Wallet:      "0x00anything",
			NftId:       "0x00anything",
		}
		assert.Nil(t, obj.IsValid())
	})

	t.Run("sucesso se data de contrato não é informada", func(t *testing.T) {
		obj := Aluno{
			ID:          uuid.New(),
			PessoaID:    uuid.New(),
			StatusAluno: StatusAluno("ATIVO"),
			Wallet:      "0x00anything",
			NftId:       "0x00anything",
		}
		assert.Nil(t, obj.IsValid())
	})
}
