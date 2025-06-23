package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	event_pkg "github.com/ggialluisi/fdqf-ms/pessoa/pkg/event_dispatcher"
)

type PessoaChangedLogOnlyHandler struct {
	MsgPrefix string
}

func NewPessoaChangedLogOnlyHandler(msgPrefix string) *PessoaChangedLogOnlyHandler {
	return &PessoaChangedLogOnlyHandler{
		MsgPrefix: msgPrefix,
	}
}

func (h *PessoaChangedLogOnlyHandler) Handle(event event_pkg.EventInterface, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Pessoa Changed: %v", event.GetPayload())
	jsonOutput, err := json.Marshal(event.GetPayload())
	if err != nil {
		log.Default().Printf("Error marshalling payload: %v", err)
		return
	}

	//do the log
	log.Default().Println("Log Only Handler - Pessoa Changed")
	log.Default().Printf("%s: %v", h.MsgPrefix, string(jsonOutput))
	log.Default().Println("end of Log Only Handler - Pessoa Changed")

}
