package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Pessoa struct {
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	ID        uuid.UUID  `gorm:"type:uuid;primary_key" json:"id"`
	Tipo      TipoPessoa `gorm:"type:varchar(20)" json:"tipo"`
	Nome      string     `gorm:"type:varchar(100)" json:"nome"`
}

type TipoPessoa string

const (
	PessoaFisica   TipoPessoa = "FISICA"
	PessoaJuridica TipoPessoa = "JURIDICA"
)

func NewPessoa(itemID *uuid.UUID, tipo TipoPessoa, nome string) (*Pessoa, error) {
	pessoa := &Pessoa{
		ID:   *itemID,
		Tipo: tipo,
		Nome: nome,
	}
	err := pessoa.IsValid()
	if err != nil {
		return nil, err
	}
	return pessoa, nil
}

func (p *Pessoa) IsValid() error {
	if &p.ID == nil || p.ID == uuid.Nil {
		return errors.New("invalid id")
	}
	if p.Tipo == "" {
		return errors.New("invalid tipo")
	}
	if p.Tipo != PessoaFisica && p.Tipo != PessoaJuridica {
		return errors.New("invalid tipo")
	}
	if p.Nome == "" {
		return errors.New("invalid name")
	}
	return nil
}
