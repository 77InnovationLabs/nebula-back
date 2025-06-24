package event

import (
	"time"

	events_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
)

type AlunoChanged struct {
	Name    string
	Payload interface{}
}

func NewAlunoChanged() *AlunoChanged {
	return &AlunoChanged{
		Name: "AlunoChanged",
	}
}

func (e *AlunoChanged) GetName() string {
	return e.Name
}

func (e *AlunoChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *AlunoChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *AlunoChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*AlunoChanged)(nil)
