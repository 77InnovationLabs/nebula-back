package event

import (
	"time"

	events_pkg "github.com/ggialluisi/fdqf-ms/curso/pkg/event_dispatcher"
)

type ItemModuloChanged struct {
	Name    string
	Payload interface{}
}

func NewItemModuloChanged() *AlunoChanged {
	return &AlunoChanged{
		Name: "ItemModuloChanged",
	}
}

func (e *ItemModuloChanged) GetName() string {
	return e.Name
}

func (e *ItemModuloChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *ItemModuloChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *ItemModuloChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*AlunoChanged)(nil)
