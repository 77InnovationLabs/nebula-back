package dto

import (
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	"github.com/google/uuid"
)

type TelefoneInputDTO struct {
	PessoaID  string `json:"pessoa_id"`
	DDD       string `json:"ddd"`
	Numero    string `json:"numero"`
	Principal bool
}

type TelefoneOutputDTO struct {
	ID        uuid.UUID `json:"id"`
	PessoaID  uuid.UUID `json:"pessoa_id"`
	DDD       string    `json:"ddd"`
	Numero    string    `json:"numero"`
	Principal bool
}

type EmailInputDTO struct {
	PessoaID  string `json:"pessoa_id"`
	Endereco  string `json:"endereco"`
	Principal bool   `json:"principal"`
}

type EmailOutputDTO struct {
	ID        uuid.UUID `json:"id"`
	PessoaID  uuid.UUID `json:"pessoa_id"`
	Endereco  string    `json:"endereco"`
	Principal bool      `json:"principal"`
}

type PessoaInputDTO struct {
	Tipo      entity.TipoPessoa `json:"tipo"`
	Nome      string            `json:"nome"`
	Documento string            `json:"documento"`
}

type PessoaNomeEmailInputDTO struct {
	Nome  string `json:"nome"`
	Email string `json:"email"`
}

type PessoaOutputDTO struct {
	ID        uuid.UUID         `json:"id"`
	Tipo      entity.TipoPessoa `json:"tipo"`
	Nome      string            `json:"nome"`
	Documento string            `json:"documento"`
}

type PessoaAgregadoOutputDTO struct {
	ID        uuid.UUID         `json:"id"`
	Tipo      entity.TipoPessoa `json:"tipo"`
	Nome      string            `json:"nome"`
	Documento string            `json:"documento"`
	Telefones []TelefoneOutputDTO
	Emails    []EmailOutputDTO
	Enderecos []EnderecoOutputDTO
}

type EnderecoInputDTO struct {
	PessoaID   string `json:"pessoa_id"`
	Logradouro string `json:"logradouro"`
	Numero     string `json:"numero"`
	CEP        string `json:"cep"`
	Bairro     string `json:"bairro"`
	Cidade     string `json:"cidade"`
	Estado     string `json:"estado"`
	Principal  bool   `json:"principal"`
	SemNumero  bool   `json:"sem_numero"`
}

type EnderecoOutputDTO struct {
	ID         uuid.UUID `json:"id"`
	PessoaID   uuid.UUID `json:"pessoa_id"`
	Logradouro string    `json:"logradouro"`
	Numero     string    `json:"numero"`
	CEP        string    `json:"cep"`
	Bairro     string    `json:"bairro"`
	Cidade     string    `json:"cidade"`
	Estado     string    `json:"estado"`
	Principal  bool      `json:"principal"`
	SemNumero  bool      `json:"sem_numero"`
}
