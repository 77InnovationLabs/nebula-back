package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Curso struct {
	ID        uuid.UUID `gorm:"type:uuid;primary_key" json:"id"`
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime"`
	Nome      string    `gorm:"type:varchar(100)" json:"nome"`
	Descricao string    `gorm:"type:varchar(1000)" json:"descricao"`
	Modulos   []Modulo  `gorm:"foreignKey:CursoID" json:"modulos"`
}

func NewCurso(itemID *uuid.UUID, nome string, descricao string) (*Curso, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	curso := &Curso{
		ID:        *itemID,
		Nome:      nome,
		Descricao: descricao,
	}
	err := curso.IsValid()
	if err != nil {
		return nil, err
	}
	return curso, nil
}

func (p *Curso) IsValid() error {
	if p.Nome == "" {
		return errors.New("invalid nome")
	}
	if p.Descricao == "" {
		return errors.New("invalid descricao")
	}
	return nil
}

func (p *Curso) IsValidDeep() error {
	err := p.IsValid()
	if err != nil {
		return err
	}
	for _, t := range p.Modulos {
		err = t.IsValid()
		if err != nil {
			return err
		}
	}
	return nil
}
