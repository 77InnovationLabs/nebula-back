package dto

import (
	"github.com/google/uuid"
)

type PessoaInputDTO struct {
	ID   string `json:"id"`
	Tipo string `json:"tipo"`
	Nome string `json:"nome"`
}

type PessoaOutputDTO struct {
	ID   uuid.UUID `json:"id"`
	Tipo string    `json:"tipo"`
	Nome string    `json:"nome"`
}
