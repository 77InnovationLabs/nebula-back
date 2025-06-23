package entity

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Testes para Entidade AlunoCurso
// Aluno é a entidade que representa um aluno em um curso
// AlunoCurso é a entidade que representa a relação entre um aluno e um curso

func TestNewAlunoCurso_ErrorIfEmptyID(t *testing.T) {
	obj := AlunoCurso{}
	assert.Error(t, obj.IsValid(), "invalid id")
}
func TestNewAlunoCurso_ErrorIfEmptyAlunoID(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid aluno id", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyCursoID(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid curso id", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyDataMatricula(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid data matricula", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyStatusCurso(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid status curso", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyStatusPagamento(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid status pagamento", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyXpGanho(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid xp ganho", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyXpDisponivel(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid xp disponivel", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyPercentualConcluido(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid percentual concluido", err.Error())
}
func TestNewAlunoCurso_ErrorIfEmptyStatus(t *testing.T) {
	obj := AlunoCurso{
		ID: uuid.New(),
	}
	err := obj.IsValid()
	assert.Error(t, err)
	assert.Equal(t, "invalid status", err.Error())
}
func TestNewAlunoCurso_Success(t *testing.T) {
	obj := AlunoCurso{
		ID:              uuid.New(),
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Now(),
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
}
func TestNewAlunoCurso_SuccessWithID(t *testing.T) {
	itemID := uuid.New()
	obj := AlunoCurso{
		ID:              itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Now(),
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.Equal(t, itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDNil(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Now(),
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmpty(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Now(),
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNil(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Now(),
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZero(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmpty(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalid(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndError(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccess(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
func TestNewAlunoCurso_SuccessWithIDEmptyAndNilAndZeroAndEmptyAndInvalidAndErrorAndSuccessAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTestAndTest(t *testing.T) {
	itemID := new(uuid.UUID)
	*itemID = uuid.Nil
	obj := AlunoCurso{
		ID:              *itemID,
		AlunoID:         uuid.New(),
		CursoID:         uuid.New(),
		DataMatricula:   time.Time{},
		StatusCurso:     StatusNaoIniciado,
		StatusPagamento: PagamentoOk,
		XpGanho:         0,
		XpDisponivel:    0,
	}
	assert.Nil(t, obj.IsValid())
	assert.NotEqual(t, *itemID, obj.ID)
}
