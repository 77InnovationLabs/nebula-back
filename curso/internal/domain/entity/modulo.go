package entity

import (
	"time"

	"errors"

	"github.com/google/uuid"
)

type Modulo struct {
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime"`
	ID        uuid.UUID `gorm:"type:uuid;primary_key"`
	CursoID   uuid.UUID `gorm:"type:uuid"`

	Nome      string `gorm:"type:varchar(100)" json:"nome"`
	Descricao string `gorm:"type:varchar(1000)" json:"descricao"`
}

func NewModulo(parentID uuid.UUID, itemID *uuid.UUID, nome string, descricao string) (*Modulo, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	modulo := &Modulo{
		ID:        *itemID,
		CursoID:   parentID,
		Nome:      nome,
		Descricao: descricao,
	}

	err := modulo.IsValid()
	if err != nil {
		return nil, err
	}
	return modulo, nil
}

func (o *Modulo) IsValid() error {
	if o.ID.String() == "" {
		return errors.New("invalid id")
	}

	if o.CursoID.String() == "" {
		return errors.New("invalid curso id")
	}

	if o.Nome == "" {
		return errors.New("invalid nome")
	}

	if o.Descricao == "" {
		return errors.New("invalid descricao")
	}

	return nil
}
