package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Pessoa struct {
	CreatedAt time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
	ID        uuid.UUID  `gorm:"type:uuid;primary_key" json:"id"`
	Tipo      TipoPessoa `gorm:"type:varchar(20)" json:"tipo"`
	Nome      string     `gorm:"type:varchar(100)" json:"nome"`
	Documento string     `gorm:"type:varchar(20)" json:"documento"`
	Enderecos []Endereco `gorm:"foreignKey:PessoaID" json:"enderecos"`
	Telefones []Telefone `gorm:"foreignKey:PessoaID" json:"telefones"`
	Emails    []Email    `gorm:"foreignKey:PessoaID" json:"emails"`
}

type TipoPessoa string

const (
	PessoaFisica   TipoPessoa = "FISICA"
	PessoaJuridica TipoPessoa = "JURIDICA"
)

func NewPessoa(itemID *uuid.UUID, tipo TipoPessoa, nome string, documento string) (*Pessoa, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	pessoa := &Pessoa{
		ID:        *itemID,
		Tipo:      tipo,
		Nome:      nome,
		Documento: documento,
	}
	err := pessoa.IsValid()
	if err != nil {
		return nil, err
	}
	return pessoa, nil
}

func (p *Pessoa) IsValid() error {
	if p.Tipo == "" {
		return errors.New("invalid tipo")
	}
	if p.Tipo != PessoaFisica && p.Tipo != PessoaJuridica {
		return errors.New("invalid tipo")
	}
	if p.Nome == "" {
		return errors.New("invalid name")
	}
	if p.Documento == "" {
		return errors.New("invalid document")
	}
	return nil
}

func (p *Pessoa) IsValidDeep() error {
	err := p.IsValid()
	if err != nil {
		return err
	}
	for _, e := range p.Enderecos {
		err = e.IsValid()
		if err != nil {
			return err
		}
	}
	for _, t := range p.Telefones {
		err = t.IsValid()
		if err != nil {
			return err
		}
	}
	for _, t := range p.Emails {
		err = t.IsValid()
		if err != nil {
			return err
		}
	}
	return nil
}

// func (o *Order) CalculateFinalPrice() error {
// 	o.FinalPrice = o.Price + o.Tax
// 	err := o.IsValid()
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
