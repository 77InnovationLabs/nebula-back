package event

import (
	"time"

	events_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
)

type CursoChanged struct {
	Name    string
	Payload interface{}
}

func NewCursoChanged() *CursoChanged {
	return &CursoChanged{
		Name: "CursoChanged",
	}
}

func (e *CursoChanged) GetName() string {
	return e.Name
}

func (e *CursoChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *CursoChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *CursoChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*CursoChanged)(nil)
