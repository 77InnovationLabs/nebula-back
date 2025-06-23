package entity

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Aluno struct {
	ID          uuid.UUID   `gorm:"type:uuid;primary_key" json:"id"`
	CreatedAt   time.Time   `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt   time.Time   `json:"updated_at" gorm:"autoUpdateTime"`
	PessoaID    uuid.UUID   `gorm:"type:uuid" json:"pessoa_id"`
	Pessoa      Pessoa      `json:"pessoa"`
	DataInicio  *time.Time  `gorm:"type:date" json:"data_inicio"`
	XpTotal     int64       `gorm:"type:int" json:"xp_total"`
	NftId       string      `gorm:"type:varchar(100)" json:"nft_id"`
	StatusAluno StatusAluno `gorm:"type:varchar(20)" json:"status_aluno"`
	Wallet      string      `gorm:"type:varchar(200)" json:"wallet"`
}

type StatusAluno string

const (
	StatusAlunoAtivo   StatusAluno = "ATIVO"
	StatusAlunoInativo StatusAluno = "INATIVO"
)

func NewAluno(itemID *uuid.UUID, pessoaID uuid.UUID, dataInicio *time.Time, xpTotal int64, nftId string, status StatusAluno, wallet string) (*Aluno, error) {
	if itemID == nil || *itemID == uuid.Nil {
		itemID = new(uuid.UUID)
		*itemID = uuid.New()
	}
	aluno := &Aluno{
		ID:          *itemID,
		PessoaID:    pessoaID,
		DataInicio:  dataInicio,
		XpTotal:     xpTotal,
		NftId:       nftId,
		StatusAluno: status,
		Wallet:      wallet,
	}
	err := aluno.IsValid()
	if err != nil {
		return nil, err
	}
	return aluno, nil
}

func (p *Aluno) Nome() string {
	return p.Pessoa.Nome
}

func (p *Aluno) TipoPessoa() TipoPessoa {
	return p.Pessoa.Tipo
}

func (p *Aluno) IsValid() error {
	if p.PessoaID == uuid.Nil {
		return errors.New("invalid pessoa")
	}
	if p.DataInicio.IsZero() {
		return errors.New("invalid data_inicio")
	}
	if p.StatusAluno != StatusAlunoAtivo && p.StatusAluno != StatusAlunoInativo {
		return errors.New("invalid status: " + string(p.StatusAluno))
	}
	if p.NftId == "" {
		return errors.New("invalid ntf_id")
	}
	if p.Wallet == "" {
		return errors.New("invalid wallet")
	}
	return nil
}

func (p *Aluno) IsValidDeep() error {
	// err := p.IsValid()
	// if err != nil {
	// 	return err
	// }
	// for _, t := range p.Modulos {
	// 	err = t.IsValid()
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

// func (o *Order) CalculateFinalPrice() error {
// 	o.FinalPrice = o.Price + o.Tax
// 	err := o.IsValid()
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
