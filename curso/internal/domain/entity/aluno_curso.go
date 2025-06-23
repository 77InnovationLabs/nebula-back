package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type StatusCurso string

const (
	StatusNaoIniciado StatusCurso = "nao_iniciado"
	StatusEmAndamento StatusCurso = "em_andamento"
	StatusAprovado    StatusCurso = "aprovado"
	StatusCancelado   StatusCurso = "cancelado"
)

type StatusPagamento string

const (
	PagamentoOk       StatusPagamento = "ok"
	PagamentoPendente StatusPagamento = "pendente"
)

type AlunoCurso struct {
	ID                  uuid.UUID       `gorm:"type:uuid;primary_key" json:"id"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
	AlunoID             uuid.UUID       `gorm:"type:uuid" json:"aluno_id"`
	Aluno               Aluno           `gorm:"foreignKey:AlunoID;references:ID" json:"aluno"`
	CursoID             uuid.UUID       `gorm:"type:uuid" json:"curso_id"`
	Curso               Curso           `gorm:"foreignKey:CursoID;references:ID" json:"curso"`
	DataMatricula       time.Time       `gorm:"type:date" json:"data_matricula"`
	PercentualConcluido float32         `gorm:"type:numeric" json:"percentual_concluido"`
	StatusCurso         StatusCurso     `gorm:"type:varchar(20)" json:"status_curso"`
	StatusPagamento     StatusPagamento `gorm:"type:varchar(20)" json:"status_pagamento"`
	XpGanho             int64           `gorm:"type:int" json:"xp_ganho"`
	XpDisponivel        int64           `gorm:"type:int" json:"xp_disponivel"`
}

func NewAlunoCurso(itemID *uuid.UUID, alunoID uuid.UUID, cursoID uuid.UUID) (*AlunoCurso, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}

	print("alunoID: ", alunoID.String())
	print("cursoID: ", cursoID.String())

	aluno_curso := &AlunoCurso{
		ID:                  uuid.New(),
		AlunoID:             alunoID,
		CursoID:             cursoID,
		DataMatricula:       time.Now(),
		StatusCurso:         StatusNaoIniciado,
		StatusPagamento:     PagamentoOk,
		PercentualConcluido: 0,
	}
	err := aluno_curso.IsValid()
	if err != nil {
		return nil, err
	}
	return aluno_curso, nil
}

func (p *AlunoCurso) IsValid() error {
	if p.AlunoID == uuid.Nil {
		return errors.New("invalid aluno")
	}
	if p.CursoID == uuid.Nil {
		return errors.New("invalid curso")
	}
	if p.DataMatricula.IsZero() {
		return errors.New("invalid data_matricula")
	}
	if p.PercentualConcluido < 0 || p.PercentualConcluido > 100 {
		return errors.New("invalid percentual_concluido")
	}
	if p.StatusCurso != StatusNaoIniciado && p.StatusCurso != StatusEmAndamento && p.StatusCurso != StatusAprovado && p.StatusCurso != StatusCancelado {
		return errors.New("invalid status")
	}
	if p.StatusPagamento != PagamentoOk && p.StatusPagamento != PagamentoPendente {
		return errors.New("invalid status")
	}
	if p.XpGanho < 0 {
		return errors.New("invalid xp_ganho")
	}
	if p.XpDisponivel < 0 {
		return errors.New("invalid xp_disponivel")
	}

	return nil
}
