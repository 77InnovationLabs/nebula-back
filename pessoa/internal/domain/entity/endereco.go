package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Endereco struct {
	CreatedAt  time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt  time.Time `json:"updated_at" gorm:"autoUpdateTime"`
	ID         uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	PessoaID   uuid.UUID `gorm:"type:uuid" json:"pessoa_id"`
	Logradouro string    `gorm:"type:varchar(100)" json:"logradouro"`
	Numero     string    `gorm:"type:varchar(20)" json:"numero"`
	CEP        string    `gorm:"type:varchar(20)" json:"cep"`
	Bairro     string    `gorm:"type:varchar(50)" json:"bairro"`
	Cidade     string    `gorm:"type:varchar(50)" json:"cidade"`
	Estado     string    `gorm:"type:varchar(2)" json:"estado"`
	Principal  bool      `json:"principal"`
	SemNumero  bool      `json:"sem_numero"`
}

func NewEndereco(parentID uuid.UUID, itemID *uuid.UUID, logradouro string, numero string, cep string, bairro string, cidade string, estado string, principal bool, semNumero bool) (*Endereco, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	endereco := &Endereco{
		ID:         *itemID,
		PessoaID:   parentID,
		Logradouro: logradouro,
		Numero:     numero,
		CEP:        cep,
		Bairro:     bairro,
		Cidade:     cidade,
		Estado:     estado,
		Principal:  principal,
		SemNumero:  semNumero,
	}
	err := endereco.IsValid()
	if err != nil {
		return nil, err
	}
	return endereco, nil
}

func (e *Endereco) IsValid() error {
	if e.Logradouro == "" {
		return errors.New("invalid logradouro")
	}
	if e.CEP == "" {
		return errors.New("invalid cep")
	}
	if e.Bairro == "" {
		return errors.New("invalid bairro")
	}
	if e.Cidade == "" {
		return errors.New("invalid cidade")
	}
	if e.Estado == "" {
		return errors.New("invalid estado")
	}
	return nil
}
