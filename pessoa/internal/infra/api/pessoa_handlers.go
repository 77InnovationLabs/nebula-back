package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/dto"
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/repository"
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/usecase"
	"github.com/ggialluisi/fdqf-ms/pessoa/pkg/event_dispatcher"
	"github.com/google/uuid"
)

type PessoaHandlers struct {
	EventDispatcher    event_dispatcher.EventDispatcherInterface
	PessoaRepository   repository.PessoaRepositoryInterface
	PessoaChangedEvent event_dispatcher.EventInterface
}

func NewPessoaHandlers(
	EventDispatcher event_dispatcher.EventDispatcherInterface,
	PessoaRepository repository.PessoaRepositoryInterface,
	PessoaChangedEvent event_dispatcher.EventInterface,
) *PessoaHandlers {
	return &PessoaHandlers{
		EventDispatcher:    EventDispatcher,
		PessoaRepository:   PessoaRepository,
		PessoaChangedEvent: PessoaChangedEvent,
	}
}

// region handlers de Pessoa

// CreatePessoa godoc
// @Summary      Save a pessoa
// @Description  Insert or Update a pessoa
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /pessoas [post]
func (h *PessoaHandlers) CreatePessoa(w http.ResponseWriter, r *http.Request) {

	var dto dto.PessoaInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteCreatePessoa(dto)
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

// CreatePessoaNomeEmail godoc
// @Summary      Save a pessoa given name and email
// @Description  Insert a pessoa given name and email
// @Tags         pessoas new-name-email
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /pessoas/v1 [post]
func (h *PessoaHandlers) CreatePessoaNomeEmail(w http.ResponseWriter, r *http.Request) {

	var dto dto.PessoaNomeEmailInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteCreatePessoaNomeEmail(dto)
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

// UpdatePessoa godoc
// @Summary      Save a pessoa
// @Description  Insert or Update a pessoa
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /pessoas/{id} [put]
func (h *PessoaHandlers) UpdatePessoa(w http.ResponseWriter, r *http.Request) {
	// id := chi.URLParam(r, "id")
	id := r.PathValue("id")

	log.Default().Println("UpdatePessoa - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.PessoaInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteUpdatePessoa(id, dto)
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

// GetPessoa godoc
// @Summary      Get a pessoa pelo ID
// @Description  Get a pessoa by ID
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /pessoas/{id} [get]
func (h *PessoaHandlers) GetPessoa(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	log.Default().Println("GetPessoa - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id_uuid, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	obj, err := h.PessoaRepository.GetPessoa(id_uuid)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeletePessoa godoc
// @Summary      Delete a pessoa pelo ID
// @Description  Delete a pessoa by ID
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /pessoas/{id} [delete]
func (h *PessoaHandlers) DeletePessoa(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	id_uuid, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = h.PessoaRepository.DeletePessoa(id_uuid)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// FindAllPessoa godoc
// @Summary      Find all pessoas
// @Description  Find all pessoas
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Param        page      query     string  false  "page number"
// @Param        limit     query     string  false  "limit"
// @Param        sort      query     string  false  "sort"
// @Success      200
// @Failure      404
// @Failure      500
// @Router       /pessoas [get]
func (h *PessoaHandlers) GetPessoas(w http.ResponseWriter, r *http.Request) {
	page := r.URL.Query().Get("page")
	limit := r.URL.Query().Get("limit")

	pageInt, err := strconv.Atoi(page)
	if err != nil {
		pageInt = 0
	}
	limitInt, err := strconv.Atoi(limit)
	if err != nil {
		limitInt = 0
	}
	sort := r.URL.Query().Get("sort")

	itens, err := h.PessoaRepository.FindAllPessoas(pageInt, limitInt, sort)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de Endereco

// CreateEndereco godoc
// @Summary      Save a endereco
// @Description  Insert or Update a endereco
// @Tags         enderecos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "endereco ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /enderecos [post]
func (h *PessoaHandlers) CreateEndereco(w http.ResponseWriter, r *http.Request) {
	var dto dto.EnderecoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteCreateEndereco(dto)
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

// UpdateEndereco godoc
// @Summary      Save a endereco
// @Description  Insert or Update a endereco
// @Tags         enderecos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "endereco ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /enderecos/{id} [put]
func (h *PessoaHandlers) UpdateEndereco(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.EnderecoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteUpdateEndereco(id, dto)
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

// GetEndereco godoc
// @Summary      Get a endereco pelo ID
// @Description  Get a endereco by ID
// @Tags         enderecos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "endereco ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /enderecos/{id} [get]
func (h *PessoaHandlers) GetEndereco(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	obj, err := ucPessoa.ExecuteGetEndereco(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteEndereco godoc
// @Summary      Delete a endereco pelo ID
// @Description  Delete a endereco by ID
// @Tags         enderecos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "endereco ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /enderecos/{id} [delete]
func (h *PessoaHandlers) DeleteEndereco(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	err = ucPessoa.ExecuteDeleteEndereco(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetEnderecosDaPessoa godoc
// @Summary      Get enderecos da pessoa pelo ID
// @Description  Get enderecos da pessoa by ID
// @Tags         enderecos
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /pessoas/{parent}/enderecos [get]
func (h *PessoaHandlers) GetEnderecosDaPessoa(w http.ResponseWriter, r *http.Request) {
	parent_id := r.PathValue("parent")
	if parent_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	itens, err := ucPessoa.ExecuteGetEnderecosDaPessoa(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de Telefone

// CreateTelefone godoc
// @Summary      Save a telefone
// @Description  Insert a telefone
// @Tags         telefones
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "telefone ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /telefones [post]
func (h *PessoaHandlers) CreateTelefone(w http.ResponseWriter, r *http.Request) {
	var dto dto.TelefoneInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteCreateTelefone(dto)
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

// UpdateTelefone godoc
// @Summary      Save a telefone
// @Description  Insert or Update a telefone
// @Tags         telefones
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "telefone ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /telefones/{id} [put]
func (h *PessoaHandlers) UpdateTelefone(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.TelefoneInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteUpdateTelefone(id, dto)
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

// GetTelefone godoc
// @Summary      Get a telefone pelo ID
// @Description  Get a telefone by ID
// @Tags         telefones
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "telefone ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /telefones/{id} [get]
func (h *PessoaHandlers) GetTelefone(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	obj, err := ucPessoa.ExecuteGetTelefone(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteTelefone godoc
// @Summary      Delete a telefone pelo ID
// @Description  Delete a telefone by ID
// @Tags         telefones
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "telefone ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /telefones/{id} [delete]
func (h *PessoaHandlers) DeleteTelefone(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	err = ucPessoa.ExecuteDeleteTelefone(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetTelefonesDaPessoa godoc
// @Summary      Get telefones da pessoa pelo ID
// @Description  Get telefones da pessoa by ID
// @Tags         telefones
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /pessoas/{parent}/telefones [get]
func (h *PessoaHandlers) GetTelefonesDaPessoa(w http.ResponseWriter, r *http.Request) {
	parent_id := r.PathValue("parent")
	if parent_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	itens, err := ucPessoa.ExecuteGetTelefonesDaPessoa(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de Email

// CreateEmail godoc
// @Summary      Save an email
// @Description  Insert an email
// @Tags         emails
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "email ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /emails [post]
func (h *PessoaHandlers) CreateEmail(w http.ResponseWriter, r *http.Request) {
	var dto dto.EmailInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteCreateEmail(dto)
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

// UpdateEmail godoc
// @Summary      Save an email
// @Description  Insert or Update an email
// @Tags         emails
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "email ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /emails/{id} [put]
func (h *PessoaHandlers) UpdateEmail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.EmailInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	output, err := ucPessoa.ExecuteUpdateEmail(id, dto)
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

// GetEmail godoc
// @Summary      Get an email pelo ID
// @Description  Get an email by ID
// @Tags         emails
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "email ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /emails/{id} [get]
func (h *PessoaHandlers) GetEmail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	obj, err := ucPessoa.ExecuteGetEmail(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteEmail godoc
// @Summary      Delete an email pelo ID
// @Description  Delete an email by ID
// @Tags         emails
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "email ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /emails/{id} [delete]
func (h *PessoaHandlers) DeleteEmail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	err = ucPessoa.ExecuteDeleteEmail(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetEmailsDaPessoa godoc
// @Summary      Get emails da pessoa pelo ID
// @Description  Get emails da pessoa by ID
// @Tags         emails
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "pessoa ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /pessoas/{parent}/emails [get]
func (h *PessoaHandlers) GetEmailsDaPessoa(w http.ResponseWriter, r *http.Request) {
	parent_id := r.PathValue("parent")
	if parent_id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := uuid.Parse(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository, h.PessoaChangedEvent, h.EventDispatcher)
	itens, err := ucPessoa.ExecuteGetEmailsDaPessoa(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion
