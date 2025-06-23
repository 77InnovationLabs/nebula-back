package event

import (
	"time"

	events_pkg "github.com/ggialluisi/fdqf-ms/pessoa/pkg/event_dispatcher"
)

type PessoaChanged struct {
	Name    string
	Payload interface{}
}

func NewPessoaChanged() *PessoaChanged {
	return &PessoaChanged{
		Name: "PessoaChanged",
	}
}

func (e *PessoaChanged) GetName() string {
	return e.Name
}

func (e *PessoaChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *PessoaChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *PessoaChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*PessoaChanged)(nil)
