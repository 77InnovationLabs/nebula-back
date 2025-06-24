package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/dto"
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/repository"
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/usecase"
	"github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
	"github.com/google/uuid"
)

type CursoHandlers struct {
	EventDispatcher        event_dispatcher.EventDispatcherInterface
	CursoRepository        repository.CursoRepositoryInterface
	CursoChangedEvent      event_dispatcher.EventInterface
	ModuloChangedEvent     event_dispatcher.EventInterface
	AlunoChangedEvent      event_dispatcher.EventInterface
	AlunoCursoChangedEvent event_dispatcher.EventInterface
	ItemModuloChangedEvent event_dispatcher.EventInterface
	PessoaRepository       repository.PessoaRepositoryInterface
}

func NewCursoHandlers(
	EventDispatcher event_dispatcher.EventDispatcherInterface,
	CursoRepository repository.CursoRepositoryInterface,
	CursoChangedEvent event_dispatcher.EventInterface,
	ModuloChangedEvent event_dispatcher.EventInterface,
	AlunoChangedEvent event_dispatcher.EventInterface,
	AlunoCursoChangedEvent event_dispatcher.EventInterface,
	ItemModuloChangedEvent event_dispatcher.EventInterface,
	PessoaRepository repository.PessoaRepositoryInterface,
) *CursoHandlers {
	return &CursoHandlers{
		EventDispatcher:        EventDispatcher,
		CursoRepository:        CursoRepository,
		CursoChangedEvent:      CursoChangedEvent,
		ModuloChangedEvent:     ModuloChangedEvent,
		AlunoChangedEvent:      AlunoChangedEvent,
		AlunoCursoChangedEvent: AlunoCursoChangedEvent,
		ItemModuloChangedEvent: ItemModuloChangedEvent,
		PessoaRepository:       PessoaRepository,
	}
}

// region handlers de Curso

// CreateCurso godoc
// @Summary      Save a curso
// @Description  Insert or Update a curso
// @Tags         cursos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /cursos [post]
func (h *CursoHandlers) CreateCurso(w http.ResponseWriter, r *http.Request) {

	var dto dto.CursoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteCreateCurso(dto)
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

// UpdateCurso godoc
// @Summary      Save a curso
// @Description  Insert or Update a curso
// @Tags         cursos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /cursos/{id} [put]
func (h *CursoHandlers) UpdateCurso(w http.ResponseWriter, r *http.Request) {
	// id := chi.URLParam(r, "id")
	id := r.PathValue("id")

	log.Default().Println("UpdateCurso - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.CursoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteUpdateCurso(id, dto)
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

// GetCurso godoc
// @Summary      Get a curso pelo ID
// @Description  Get a curso by ID
// @Tags         cursos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /cursos/{id} [get]
func (h *CursoHandlers) GetCurso(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	log.Default().Println("GetCurso - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteGetCurso(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteCurso godoc
// @Summary      Delete a curso pelo ID
// @Description  Delete a curso by ID
// @Tags         cursos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /cursos/{id} [delete]
func (h *CursoHandlers) DeleteCurso(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err := ucCurso.ExecuteDeleteCurso(id)
	if err != nil {
		log.Default().Println("DeleteCurso - Error: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// FindAllCurso godoc
// @Summary      Find all cursos
// @Description  Find all cursos
// @Tags         cursos
// @Accept       json
// @Produce      json
// @Param        page      query     string  false  "page number"
// @Param        limit     query     string  false  "limit"
// @Param        sort      query     string  false  "sort"
// @Success      200
// @Failure      404
// @Failure      500
// @Router       /cursos [get]
func (h *CursoHandlers) GetCursos(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetCursos(pageInt, limitInt, sort)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de Modulo

// CreateModulo godoc
// @Summary      Save a modulo
// @Description  Insert a modulo
// @Tags         modulos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "modulo ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /modulos [post]
func (h *CursoHandlers) CreateModulo(w http.ResponseWriter, r *http.Request) {
	var dto dto.ModuloInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteCreateModulo(dto)
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

// UpdateModulo godoc
// @Summary      Save a modulo
// @Description  Insert or Update a modulo
// @Tags         modulos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "modulo ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500	   {object}  string
// @Router       /modulos/{id} [put]
func (h *CursoHandlers) UpdateModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.ModuloInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteUpdateModulo(id, dto)
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

// GetModulo godoc
// @Summary      Get a modulo pelo ID
// @Description  Get a modulo by ID
// @Tags         modulos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "modulo ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /modulos/{id} [get]
func (h *CursoHandlers) GetModulo(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteGetModulo(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteModulo godoc
// @Summary      Delete a modulo pelo ID
// @Description  Delete a modulo by ID
// @Tags         modulos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "modulo ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /modulos/{id} [delete]
func (h *CursoHandlers) DeleteModulo(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err = ucCurso.ExecuteDeleteModulo(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetModulosDaCurso godoc
// @Summary      Get modulos da curso pelo ID
// @Description  Get modulos da curso by ID
// @Tags         modulos
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /cursos/{parent}/modulos [get]
func (h *CursoHandlers) GetModulosDaCurso(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetModulosDeCurso(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// GetPessoas godoc
// @Summary      Get pessoas
// @Description  Get pessoas
// @Tags         pessoas
// @Accept       json
// @Produce      json
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /pessoas [get]
func (h *CursoHandlers) GetPessoas(w http.ResponseWriter, r *http.Request) {
	ucPessoa := usecase.NewSavePessoaUseCase(h.PessoaRepository)
	itens, err := ucPessoa.ExecuteGetPessoas()
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de Aluno

// type CursoHandlers struct {
// 	EventDispatcher   event_dispatcher.EventDispatcherInterface
// 	CursoRepository   repository.CursoRepositoryInterface
// 	AlunoChangedEvent event_dispatcher.EventInterface
// 	PessoaRepository  repository.PessoaRepositoryInterface
// }

// func NewCursoHandlers(
// 	EventDispatcher event_dispatcher.EventDispatcherInterface,
// 	CursoRepository repository.CursoRepositoryInterface,
// 	AlunoChangedEvent event_dispatcher.EventInterface,
// 	PessoaRepository repository.PessoaRepositoryInterface,
// ) *CursoHandlers {
// 	return &CursoHandlers{
// 		EventDispatcher:   EventDispatcher,
// 		CursoRepository:   CursoRepository,
// 		AlunoChangedEvent: AlunoChangedEvent,
// 		PessoaRepository:  PessoaRepository,
// 	}
// }

// CreateAluno godoc
// @Summary      Save a aluno
// @Description  Insert an Aluno
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /alunos [post]
func (h *CursoHandlers) CreateAluno(w http.ResponseWriter, r *http.Request) {

	var dto dto.AlunoNewInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteCreateAluno(dto)
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

// UpdateAluno godoc
// @Summary      Save a aluno
// @Description  Insert or Update a aluno
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500       {object}  string
// @Router       /alunos/{id} [put]
func (h *CursoHandlers) UpdateAluno(w http.ResponseWriter, r *http.Request) {
	// id := chi.URLParam(r, "id")
	id := r.PathValue("id")

	log.Default().Println("UpdateAluno - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.AlunoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteUpdateAluno(id, dto)
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

// GetAluno godoc
// @Summary      Get a aluno pelo ID
// @Description  Get a aluno by ID
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunos/{id} [get]
func (h *CursoHandlers) GetAluno(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	log.Default().Println("GetAluno - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteGetAluno(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// GetAlunoByWallet godoc
// @Summary      Get a aluno pela sua wallet
// @Description  Get a aluno by wallet address
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunos/by-wallet/{id} [get]
func (h *CursoHandlers) GetAlunoByWallet(w http.ResponseWriter, r *http.Request) {
	wallet := r.PathValue("wallet")
	log.Default().Println("GetAlunoBayWallet - Wallet: ", wallet)

	if wallet == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteGetAlunoByWallet(wallet)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteAluno godoc
// @Summary      Delete a aluno pelo ID
// @Description  Delete a aluno by ID
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /alunos/{id} [delete]
func (h *CursoHandlers) DeleteAluno(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err := ucCurso.ExecuteDeleteAluno(id)
	if err != nil {
		log.Default().Println("DeleteAluno - Error: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// FindAllAluno godoc
// @Summary      Find all alunos
// @Description  Find all alunos
// @Tags         alunos
// @Accept       json
// @Produce      json
// @Param        page      query     string  false  "page number"
// @Param        limit     query     string  false  "limit"
// @Param        sort      query     string  false  "sort"
// @Success      200
// @Failure      404
// @Failure      500
// @Router       /alunos [get]
func (h *CursoHandlers) GetAlunos(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetAlunos(pageInt, limitInt, sort)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// endregion

// region handlers de AlunoCurso
// CreateAlunoCurso godoc
// @Summary      Save a alunoCurso
// @Description  Insert or Update a alunoCurso
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "alunoCurso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure	  500       {object}  string
// @Router       /alunocursos [post]
func (h *CursoHandlers) CreateAlunoCurso(w http.ResponseWriter, r *http.Request) {
	var dto dto.AlunoCursoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteCreateAlunoCurso(dto)
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

// UpdateAlunoCurso godoc
// @Summary      Save a alunoCurso
// @Description  Insert or Update a alunoCurso
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        id        	path      string                  true  "alunoCurso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure	  500       {object}  string
// @Router       /alunocursos/{id} [put]
func (h *CursoHandlers) UpdateAlunoCurso(w http.ResponseWriter, r *http.Request) {
	// id := chi.URLParam(r, "id")
	id := r.PathValue("id")

	log.Default().Println("UpdateAlunoCurso - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var dto dto.AlunoCursoInputDTO
	err := json.NewDecoder(r.Body).Decode(&dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteUpdateAlunoCurso(id, dto)
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

// GetAlunoCurso godoc
// @Summary      Get a alunoCurso pelo ID
// @Description  Get a alunoCurso by ID
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "alunoCurso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunocursos/{id} [get]
func (h *CursoHandlers) GetAlunoCurso(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	log.Default().Println("GetAlunoCurso - ID: ", id)

	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteGetAlunoCurso(id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(obj)
}

// DeleteAlunoCurso godoc
// @Summary      Delete a alunoCurso pelo ID
// @Description  Delete a alunoCurso by ID
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "alunoCurso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object}  string
// @Router       /alunocursos/{id} [delete]
func (h *CursoHandlers) DeleteAlunoCurso(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err := ucCurso.ExecuteDeleteAlunoCurso(id)
	if err != nil {
		log.Default().Println("DeleteAlunoCurso - Error: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// FindAllAlunoCurso godoc
// @Summary      Find all alunoCursos
// @Description  Find all alunoCursos
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        page      query     string  false  "page number"
// @Param        limit     query     string  false  "limit"
// @Param        sort      query     string  false  "sort"
// @Success      200
// @Failure      404
// @Failure      500
// @Router       /alunocursos [get]
func (h *CursoHandlers) GetAlunosCursos(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetAlunoCursos(pageInt, limitInt, sort)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// GetCursosDoAluno godoc
// @Summary      Get cursos do aluno pelo ID
// @Description  Get cursos do aluno by ID
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "aluno ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunos/{parent}/cursos [get]
func (h *CursoHandlers) GetCursosDoAluno(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetCursosDoAluno(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// GetAlunosDoCurso godoc
// @Summary      Get alunos do curso pelo ID
// @Description  Get alunos do curso by ID
// @Tags         alunocursos
// @Accept       json
// @Produce      json
// @Param        parent   path      string  true  "curso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /cursos/{parent}/alunos [get]
func (h *CursoHandlers) GetAlunosDoCurso(w http.ResponseWriter, r *http.Request) {
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

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	itens, err := ucCurso.ExecuteGetAlunosDoCurso(parent_id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(itens)
}

// region ItemModulo
// CreateItemModulo godoc
// @Summary      Create item modulo
// @Description  Create an item in a modulo
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        input  body      dto.ItemModuloInputDTO  true  "ItemModulo input"
// @Success      200    {object}  dto.ItemModuloOutputDTO
// @Failure      400    {object}  string
// @Failure      500    {object}  string
// @Router       /modulos/{modulo_id}/itens [post]
func (h *CursoHandlers) CreateItemModulo(w http.ResponseWriter, r *http.Request) {
	var input dto.ItemModuloInputDTO
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteCreateItemModulo(input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// GetItemModulo godoc
// @Summary      Get item modulo by ID
// @Description  Retrieve an item modulo by its ID
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "ItemModulo ID"
// @Success      200  {object}  dto.ItemModuloOutputDTO
// @Failure      400  {object}  string
// @Failure      404  {object}  string
// @Router       /itens/{id} [get]
func (h *CursoHandlers) GetItemModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteFindItemModuloByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// GetItensModulo godoc
// @Summary      Get all items from a modulo
// @Description  Retrieve all items from a given modulo
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        modulo_id   path      string  true  "Modulo ID"
// @Success      200  {array}   dto.ItemModuloOutputDTO
// @Failure      400  {object}  string
// @Failure      500  {object}  string
// @Router       /modulos/{modulo_id}/itens [get]
func (h *CursoHandlers) GetItensModulo(w http.ResponseWriter, r *http.Request) {
	moduloID := r.PathValue("modulo_id")
	if moduloID == "" {
		http.Error(w, "missing modulo_id", http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteFindItemModulosByModulo(moduloID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// UpdateItemModulo godoc
// @Summary      Update item modulo
// @Description  Update an item from a modulo
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        id    path      string                true  "ItemModulo ID"
// @Param        input body      dto.ItemModuloInputDTO    true  "Updated item"
// @Success      200   {object}  dto.ItemModuloOutputDTO
// @Failure      400   {object}  string
// @Failure      500   {object}  string
// @Router       /itens/{id} [put]
func (h *CursoHandlers) UpdateItemModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	var dto dto.ItemModuloInputDTO
	if err := json.NewDecoder(r.Body).Decode(&dto); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	obj, err := ucCurso.ExecuteUpdateItemModulo(id, dto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(obj)
}

// DeleteItemModulo godoc
// @Summary      Delete item modulo
// @Description  Delete an item modulo by ID
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "ItemModulo ID"
// @Success      200
// @Failure      400  {object}  string
// @Failure      500  {object}  string
// @Router       /itens/{id} [delete]
func (h *CursoHandlers) DeleteItemModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err := ucCurso.ExecuteDeleteItemModulo(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// MoveItemModulo godoc
// @Summary      Move item modulo
// @Description  Change the order of an item modulo
// @Tags         itemmodulo
// @Accept       json
// @Produce      json
// @Param        id      path     string  true  "ItemModulo ID"
// @Param        action  query    string  true  "Action (cima|baixo|inicio|fim)"
// @Success      200
// @Failure      400  {object}  string
// @Failure      500  {object}  string
// @Router       /itens/{id}/mover [post]
func (h *CursoHandlers) MoveItemModulo(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	action := r.URL.Query().Get("action")
	id, err := uuid.Parse(idStr)
	if err != nil || action == "" {
		http.Error(w, "invalid id or action", http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	err = ucCurso.ExecuteMoveItemModulo(id, action)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// endregion

// region handlers de AlunoCursoItemModulo

// GetAlunoCursoItemModulos godoc
// @Summary      Lista todos os itens de módulo de uma matrícula
// @Description  Get all AlunoCursoItemModulo by AlunoCurso ID
// @Tags         alunocursoitemmodulos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "AlunoCurso ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunocursos/{id}/itemmodulos [get]
func (h *CursoHandlers) GetAlunoCursoItemModulos(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteFindAlunoCursoItemModulos(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// GetAlunoCursoItemModulo godoc
// @Summary      Busca um único item de módulo de uma matrícula
// @Description  Get AlunoCursoItemModulo by ID
// @Tags         alunocursoitemmodulos
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "AlunoCursoItemModulo ID" Format(uuid)
// @Success      200
// @Failure      404
// @Failure      500  {object} string
// @Router       /alunocursoitemmodulos/{id} [get]
func (h *CursoHandlers) GetAlunoCursoItemModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteGetAlunoCursoItemModulo(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// UpdateAlunoCursoItemModulo godoc
// @Summary      Atualiza progresso, status ou campos específicos do item de módulo
// @Description  Update AlunoCursoItemModulo by ID
// @Tags         alunocursoitemmodulos
// @Accept       json
// @Produce      json
// @Param        id    path      string  true  "AlunoCursoItemModulo ID" Format(uuid)
// @Param        input body      dto.AlunoCursoItemModuloUpdateDTO true  "Campos para atualização"
// @Success      200
// @Failure      400
// @Failure      500  {object} string
// @Router       /alunocursoitemmodulos/{id} [patch]
func (h *CursoHandlers) UpdateAlunoCursoItemModulo(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	var input dto.AlunoCursoItemModuloUpdateDTO
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ucCurso := usecase.NewSaveCursoUseCase(
		h.CursoRepository,
		h.PessoaRepository,
		h.CursoChangedEvent,
		h.ModuloChangedEvent,
		h.AlunoChangedEvent,
		h.AlunoCursoChangedEvent,
		h.ItemModuloChangedEvent,
		h.EventDispatcher)
	output, err := ucCurso.ExecuteUpdateAlunoCursoItemModulo(id, input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(output)
}

// endregion
