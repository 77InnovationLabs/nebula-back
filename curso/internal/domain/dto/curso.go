package dto

import (
	"time"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"
	"github.com/google/uuid"
)

// region Modulo

type ModuloInputDTO struct {
	CursoID   string `json:"curso_id"`
	Nome      string `json:"nome"`
	Descricao string `json:"descricao"`
}

type ModuloOutputDTO struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	CursoID   uuid.UUID `json:"curso_id"`
	Nome      string    `json:"nome"`
	Descricao string    `json:"descricao"`
}

// endregion

// region Curso

type CursoInputDTO struct {
	Nome      string `json:"nome"`
	Descricao string `json:"descricao"`
}

type CursoOutputDTO struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Nome      string    `json:"nome"`
	Descricao string    `json:"descricao"`
}

type CursoAgregadoOutputDTO struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Nome      string    `json:"nome"`
	Descricao string    `json:"descricao"`
	Modulos   []ModuloOutputDTO
}

// endregion

// region Aluno
type AlunoNewInputDTO struct {
	PessoaID string `json:"pessoa_id"`
	Nome     string `json:"nome"`
	Wallet   string `json:"wallet"`
}

type AlunoInputDTO struct {
	PessoaID    string     `json:"pessoa_id"`
	DataInicio  *time.Time `json:"data_inicio"`
	XpTotal     int64      `json:"xp_total"`
	NftId       string     `json:"nft_id"`
	StatusAluno string     `json:"status_aluno"`
	Nome        string     `json:"nome"`
	Wallet      string     `json:"wallet"`
}

type AlunoOutputDTO struct {
	ID          uuid.UUID          `json:"id"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	PessoaID    uuid.UUID          `json:"pessoa_id"`
	Nome        string             `json:"nome"`
	Wallet      string             `json:"wallet"`
	TipoPessoa  entity.TipoPessoa  `json:"tipo_pessoa"`
	DataInicio  *time.Time         `json:"data_inicio"`
	XpTotal     int64              `json:"xp_total"`
	NftId       string             `json:"nft_id"`
	StatusAluno entity.StatusAluno `json:"status_aluno"`
}

type AlunoAgregadoOutputDTO struct {
	ID          uuid.UUID          `json:"id"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	PessoaID    uuid.UUID          `json:"pessoa_id"`
	DataInicio  *time.Time         `json:"data_inicio"`
	XpTotal     int64              `json:"xp_total"`
	NftId       string             `json:"nft_id"`
	StatusAluno entity.StatusAluno `json:"status_aluno"`
	Nome        string             `json:"nome"`
	Wallet      string             `json:"wallet"`
	Cursos      []CursoOutputDTO
}

// endregion

// region AlunoCurso

type AlunoCursoInputDTO struct {
	CursoID string `json:"curso_id"`
	AlunoID string `json:"aluno_id"`
}

type AlunoCursoOutputDTO struct {
	ID                  uuid.UUID              `json:"id"`
	CreatedAt           time.Time              `json:"created_at"`
	UpdatedAt           time.Time              `json:"updated_at"`
	CursoID             uuid.UUID              `json:"curso_id"`
	AlunoID             uuid.UUID              `json:"aluno_id"`
	AlunoNome           string                 `json:"aluno_nome"`
	CursoNome           string                 `json:"curso_nome"`
	CursoDescricao      string                 `json:"curso_descricao"`
	DataMatricula       time.Time              `json:"data_matricula"`
	PercentualConcluido float32                `json:"percentual_concluido"`
	StatusCurso         entity.StatusCurso     `json:"status_curso"`
	StatusPagamento     entity.StatusPagamento `json:"status_pagamento"`
}

// endregion

// region ItemModulo
type ItemModuloAulaDTO struct {
	Texto string `json:"texto"`
}

type ItemModuloContractValidationDTO struct {
	Rede             string `json:"rede"`
	EnderecoContrato string `json:"endereco_contrato"`
}

type ItemModuloInputDTO struct {
	ModuloID           string                           `json:"modulo_id"`
	Nome               string                           `json:"nome"`
	Descricao          string                           `json:"descricao"`
	EstimativaTempoMin int                              `json:"estimativa_tempo_minutos"`
	Tipo               string                           `json:"tipo"`
	Aula               *ItemModuloAulaDTO               `json:"aula,omitempty"`
	ContractValidation *ItemModuloContractValidationDTO `json:"contract_validation,omitempty"`
}

type ItemModuloOutputDTO struct {
	ID                 string                           `json:"id"`
	ModuloID           string                           `json:"modulo_id"`
	Nome               string                           `json:"nome"`
	Descricao          string                           `json:"descricao"`
	EstimativaTempoMin int                              `json:"estimativa_tempo_minutos"`
	Tipo               string                           `json:"tipo"`
	Aula               *ItemModuloAulaDTO               `json:"aula,omitempty"`
	ContractValidation *ItemModuloContractValidationDTO `json:"contract_validation,omitempty"`
	Ordem              int                              `json:"ordem"`
	CreatedAt          time.Time                        `json:"created_at"`
	UpdatedAt          time.Time                        `json:"updated_at"`
}

// endregion

// region AlunoCursoItemModulo
type AlunoCursoItemModuloResponseDTO struct {
	ID                      uuid.UUID                          `json:"id"`
	AlunoCursoID            uuid.UUID                          `json:"aluno_curso_id"`
	ItemModuloID            uuid.UUID                          `json:"item_modulo_id"`
	ItemModuloNome          string                             `json:"item_modulo_nome"`
	TipoItemModulo          entity.TipoItem                    `json:"tipo_item_modulo"`
	Status                  entity.TipoStatusItemModulo        `json:"status"`
	Progresso               float32                            `json:"progresso"`
	TempoAssistido          int64                              `json:"tempo_assistido"`
	EnderecoContratoValidar string                             `json:"endereco_contrato_validar"`
	BlockchainRedeValidacao string                             `json:"blockchain_rede_validacao"`
	BlockchainTxEnvio       string                             `json:"blockchain_tx_envio"`
	StatusValidacaoContrato entity.TipoStatusValidacaoContrato `json:"status_validacao_contrato"`
	CreatedAt               time.Time                          `json:"created_at"`
	UpdatedAt               time.Time                          `json:"updated_at"`
}

type AlunoCursoItemModuloUpdateDTO struct {
	Status                  *entity.TipoStatusItemModulo        `json:"status,omitempty"`
	Progresso               *float32                            `json:"progresso,omitempty"`
	TempoAssistido          *int64                              `json:"tempo_assistido,omitempty"`
	EnderecoContratoValidar *string                             `json:"endereco_contrato_validar,omitempty"`
	BlockchainRedeValidacao *string                             `json:"blockchain_rede_validacao,omitempty"`
	BlockchainTxEnvio       *string                             `json:"blockchain_tx_envio,omitempty"`
	StatusValidacaoContrato *entity.TipoStatusValidacaoContrato `json:"status_validacao_contrato,omitempty"`
}

// endregion
