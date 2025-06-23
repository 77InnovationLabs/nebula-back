package entity

import (
	"net/mail"
	"time"

	"errors"

	"github.com/google/uuid"
)

type Email struct {
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime"`
	ID        uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	PessoaID  uuid.UUID `gorm:"type:uuid" json:"pessoa_id"`
	Endereco  string    `gorm:"type:varchar(100)" json:"endereco"`
	Principal bool      `json:"principal"`
}

func NewEmail(parentID uuid.UUID, itemID *uuid.UUID, endereco string, principal bool) (*Email, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	email := &Email{
		ID:        *itemID,
		PessoaID:  parentID,
		Endereco:  endereco,
		Principal: principal,
	}
	err := email.IsValid()
	if err != nil {
		return nil, err
	}
	return email, nil
}

func (e *Email) IsValid() error {
	if e.Endereco == "" {
		return errors.New("invalid email")
	}
	//verifica se e.Endereco é um email
	_, err := mail.ParseAddress(e.Endereco)
	if err != nil {
		return errors.New("invalid email")
	}
	return nil
}
