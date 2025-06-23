package entity

import (
	"strconv"
	"time"

	"errors"

	"github.com/google/uuid"
)

type Telefone struct {
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime"`
	ID        uuid.UUID `gorm:"type:uuid;primary_key"`
	PessoaID  uuid.UUID `gorm:"type:uuid"`
	DDD       string    `gorm:"type:varchar(3)"`
	Numero    string    `gorm:"type:varchar(20)"`
	Principal bool
}

func NewTelefone(parentID uuid.UUID, itemID *uuid.UUID, ddd string, numero string, principal bool) (*Telefone, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	telefone := &Telefone{
		ID:        *itemID,
		PessoaID:  parentID,
		DDD:       ddd,
		Numero:    numero,
		Principal: principal,
	}
	err := telefone.IsValid()
	if err != nil {
		return nil, err
	}
	return telefone, nil
}

func (o *Telefone) IsValid() error {
	if o.ID.String() == "" {
		return errors.New("invalid id")
	}

	if o.DDD == "" {
		return errors.New("invalid ddd")
	}

	if o.Numero == "" {
		return errors.New("invalid numero")
	}

	_, err := strconv.Atoi(o.DDD)
	if err != nil {
		return errors.New("invalid ddd")
	}

	_, err = strconv.Atoi(o.Numero)
	if err != nil {
		return errors.New("invalid numero")
	}
	return nil
}
