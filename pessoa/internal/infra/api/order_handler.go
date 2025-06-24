package api

import (
	"encoding/json"
	"net/http"

	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/repository"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/usecase"
	"github.com/77InnovationLabs/nebula-back/pessoa/pkg/event_dispatcher"
)

type WebOrderHandler struct {
	EventDispatcher   event_dispatcher.EventDispatcherInterface
	OrderRepository   repository.OrderRepositoryInterface
	OrderCreatedEvent event_dispatcher.EventInterface
}

func NewWebOrderHandler(
	EventDispatcher event_dispatcher.EventDispatcherInterface,
	OrderRepository repository.OrderRepositoryInterface,
	OrderCreatedEvent event_dispatcher.EventInterface,
) *WebOrderHandler {
	return &WebOrderHandler{
		EventDispatcher:   EventDispatcher,
		OrderRepository:   OrderRepository,
		OrderCreatedEvent: OrderCreatedEvent,
	}
}

func (h *WebOrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var dto usecase.OrderInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	createOrder := usecase.NewCreateOrderUseCase(h.OrderRepository, h.OrderCreatedEvent, h.EventDispatcher)
	output, err := createOrder.Execute(dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(output)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
