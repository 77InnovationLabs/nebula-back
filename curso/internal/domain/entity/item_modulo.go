package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type TipoItem string

const (
	ItemAula             TipoItem = "aula"
	ItemContractValidate TipoItem = "contract_validation"
)

type RedeValidacao string

const (
	RedeSepolia  RedeValidacao = "sepolia"
	RedeEthereum RedeValidacao = "ethereum"
	RedeScroll   RedeValidacao = "scroll"
)

type ItemModulo struct {
	ID                 uuid.UUID                     `gorm:"type:uuid;primaryKey" json:"id"`
	ModuloID           uuid.UUID                     `gorm:"type:uuid" json:"modulo_id"`
	Nome               string                        `gorm:"type:varchar(200)" json:"nome"`
	Descricao          string                        `gorm:"type:varchar(1000)" json:"descricao"`
	EstimativaTempoMin int                           `json:"estimativa_tempo_minutos"`
	Ordem              int                           `json:"ordem"`
	Tipo               TipoItem                      `gorm:"type:varchar(30)" json:"tipo"`
	Aula               *ItemModuloAula               `gorm:"constraint:OnDelete:CASCADE" json:"aula,omitempty"`
	ContractValidation *ItemModuloContractValidation `gorm:"constraint:OnDelete:CASCADE" json:"contract_validation,omitempty"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
}

type ItemModuloAula struct {
	ItemModuloID uuid.UUID `gorm:"type:uuid;primaryKey" json:"item_modulo_id"`
	Texto        string    `gorm:"type:text" json:"texto"`
}

type ItemModuloContractValidation struct {
	ItemModuloID     uuid.UUID     `gorm:"type:uuid;primaryKey" json:"item_modulo_id"`
	Rede             RedeValidacao `gorm:"type:varchar(20)" json:"rede"`
	EnderecoContrato string        `gorm:"type:varchar(100)" json:"endereco_contrato"`
}

func NewItemModulo(moduloID uuid.UUID, itemID *uuid.UUID, nome string, descricao string, estimativaTempoMin int, ordem int, tipo TipoItem) (*ItemModulo, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	itemModulo := &ItemModulo{
		ID:                 *itemID,
		ModuloID:           moduloID,
		Nome:               nome,
		Descricao:          descricao,
		EstimativaTempoMin: estimativaTempoMin,
		Ordem:              ordem,
		Tipo:               tipo,
	}

	err := itemModulo.IsValid()
	if err != nil {
		return nil, err
	}
	return itemModulo, nil
}

func (o *ItemModulo) IsValid() error {
	if o.ID.String() == "" {
		return errors.New("invalid id")
	}
	if o.ModuloID.String() == "" {
		return errors.New("invalid modulo id")
	}
	if o.Nome == "" {
		return errors.New("invalid nome")
	}
	if o.Descricao == "" {
		return errors.New("invalid descricao")
	}
	if o.EstimativaTempoMin <= 0 {
		return errors.New("invalid estimativa tempo")
	}
	if o.Ordem <= 0 {
		return errors.New("invalid ordem")
	}
	if o.Tipo == "" {
		return errors.New("invalid tipo")
	}
	if o.Tipo != ItemAula && o.Tipo != ItemContractValidate {
		return errors.New("invalid tipo")
	}
	if o.Tipo == ItemAula && o.Aula == nil {
		return errors.New("invalid aula")
	}
	if o.Tipo == ItemContractValidate && o.ContractValidation == nil {
		return errors.New("invalid contract validation")
	}
	if o.Tipo == ItemContractValidate {
		if o.ContractValidation.Rede == "" {
			return errors.New("invalid rede")
		}
		if o.ContractValidation.EnderecoContrato == "" {
			return errors.New("invalid endereco contrato")
		}
	}
	if o.Tipo == ItemAula {
		if o.Aula.Texto == "" {
			return errors.New("invalid texto")
		}
	}
	return nil
}
