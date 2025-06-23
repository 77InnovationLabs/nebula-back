package event

import (
	"time"

	events_pkg "github.com/ggialluisi/fdqf-ms/curso/pkg/event_dispatcher"
)

type AlunoCursoChanged struct {
	Name    string
	Payload interface{}
}

func NewAlunoCursoChanged() *AlunoChanged {
	return &AlunoChanged{
		Name: "AlunoCursoChanged",
	}
}

func (e *AlunoCursoChanged) GetName() string {
	return e.Name
}

func (e *AlunoCursoChanged) GetPayload() interface{} {
	return e.Payload
}

func (e *AlunoCursoChanged) SetPayload(payload interface{}) {
	e.Payload = payload
}

func (e *AlunoCursoChanged) GetDateTime() time.Time {
	return time.Now()
}

// verifica se implementa a interface
var _ events_pkg.EventInterface = (*AlunoChanged)(nil)
