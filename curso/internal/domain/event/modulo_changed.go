package event

import (
	"time"

	events_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
)

type ModuloChanged struct {
	Name    string
	Payload interface{}
}

func NewModuloChanged() *ModuloChanged {
	return &ModuloChanged{
		Name: "ModuloChanged",
	}
}

func (e *ModuloChanged) GetName() string {
	return e.Name
}

func (e *ModuloChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *ModuloChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *ModuloChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*ModuloChanged)(nil)
