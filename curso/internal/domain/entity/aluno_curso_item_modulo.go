package entity

import (
	"time"

	"github.com/google/uuid"
)

type TipoStatusItemModulo string

const (
	TipoStatusItemModuloNaoIniciado TipoStatusItemModulo = "não iniciado"
	TipoStatusItemModuloEmAndamento TipoStatusItemModulo = "em andamento"
	TipoStatusItemModuloConcluido   TipoStatusItemModulo = "concluído"
)

type TipoStatusValidacaoContrato string

const (
	TipoStatusValidacaoContratoPendente  TipoStatusValidacaoContrato = "validação contrato pendente"
	TipoStatusValidacaoContratoConcluido TipoStatusValidacaoContrato = "validação contrato concluída"
	TipoStatusValidacaoContratoErro      TipoStatusValidacaoContrato = "validação contrato erro"
)

type AlunoCursoItemModulo struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Foreign Keys
	AlunoCursoID uuid.UUID  `gorm:"type:uuid" json:"aluno_curso_id"`
	AlunoCurso   AlunoCurso `gorm:"foreignKey:AlunoCursoID;references:ID"`

	ItemModuloID uuid.UUID  `gorm:"type:uuid" json:"item_modulo_id"`
	ItemModulo   ItemModulo `gorm:"foreignKey:ItemModuloID;references:ID"`

	// Campos comuns
	Status    TipoStatusItemModulo `gorm:"type:varchar(50)" json:"status"` // Ex: não iniciado, em andamento, concluído
	Progresso float32              `gorm:"type:numeric" json:"progresso"`  // 0-100%

	// Específicos para AULA e VIDEO
	TempoAssistido int64 `gorm:"type:int" json:"tempo_assistido"` // segundos

	// Específicos para VALIDACAO_CONTRATO
	EnderecoContratoValidar string                      `gorm:"type:varchar(255)" json:"endereco_contrato_validar"` // Endereço do contrato a ser validado
	BlockchainRedeValidacao string                      `gorm:"type:varchar(20)" json:"blockchain_rede_validacao"`  // Ex: ethereum, polygon, etc.
	BlockchainTxEnvio       string                      `gorm:"type:varchar(255)" json:"blockchain_tx_envio"`       // Hash da transação de envio do contrato
	StatusValidacaoContrato TipoStatusValidacaoContrato `gorm:"type:varchar(50)" json:"status_validacao_contrato"`  // Ex: pendente, concluída, erro
}

// TipoItemModulo retorna o tipo de ItemModulo associado.
func (p *AlunoCursoItemModulo) TipoItemModulo() TipoItem {
	return p.ItemModulo.Tipo
}
