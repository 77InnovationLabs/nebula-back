package web

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/ggialluisi/fdqf-ms/curso/internal/infra/api"
)

// SetupRoutes configura todas as rotas da aplicação.
func SetupRoutes(
	frontendURL string,
	servicePort string,
	tokenAuth *jwtauth.JWTAuth,
	jwtExpiresIn int,
	cursoApiHandlers *api.CursoHandlers,
	userApiHandlers *api.UserHandlers,
	adminPanel http.Handler,
) http.Handler {

	// Configurar o roteador Chi
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	// r.Use(middleware.Recoverer)
	r.Use(middleware.WithValue("jwt", tokenAuth))
	r.Use(middleware.WithValue("JwtExperesIn", jwtExpiresIn))

	// Configurar CORS
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{frontendURL}, // ou []string{"*"} para permitir todas as origens (cuidado em produção)
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Tempo em segundos para cachear a preflight request
	}))

	//autenticação
	r.Post("/users", userApiHandlers.CreateUser)
	r.Post("/users/generate_token", userApiHandlers.GetJWT)

	//rotas do microserviço
	r.Post("/alunos", cursoApiHandlers.CreateAluno)
	r.Get("/alunos", cursoApiHandlers.GetAlunos)
	r.Get("/alunos/{id}", cursoApiHandlers.GetAluno)
	r.Get("/alunos/by-wallet/{wallet}", cursoApiHandlers.GetAlunoByWallet)
	r.Put("/alunos/{id}", cursoApiHandlers.UpdateAluno)
	r.Delete("/alunos/{id}", cursoApiHandlers.DeleteAluno)

	r.Post("/cursos", cursoApiHandlers.CreateCurso)
	r.Get("/cursos", cursoApiHandlers.GetCursos)
	r.Get("/cursos/{id}", cursoApiHandlers.GetCurso)
	r.Put("/cursos/{id}", cursoApiHandlers.UpdateCurso)
	r.Delete("/cursos/{id}", cursoApiHandlers.DeleteCurso)

	r.Post("/modulos", cursoApiHandlers.CreateModulo)
	r.Put("/modulos/{id}", cursoApiHandlers.UpdateModulo)
	r.Get("/modulos/{id}", cursoApiHandlers.GetModulo)
	r.Delete("/modulos/{id}", cursoApiHandlers.DeleteModulo)
	r.Get("/cursos/{parent}/modulos", cursoApiHandlers.GetModulosDaCurso)

	r.Post("/alunocursos", cursoApiHandlers.CreateAlunoCurso)
	r.Put("/alunocursos/{id}", cursoApiHandlers.UpdateAlunoCurso)
	r.Get("/alunocursos/{id}", cursoApiHandlers.GetAlunoCurso)
	r.Delete("/alunocursos/{id}", cursoApiHandlers.DeleteAlunoCurso)
	r.Get("/alunocursos", cursoApiHandlers.GetAlunosCursos)
	r.Get("/alunocursos/aluno/{parent}", cursoApiHandlers.GetAlunosDoCurso)
	r.Get("/alunocursos/curso/{parent}", cursoApiHandlers.GetCursosDoAluno)

	r.Post("/modulos/{modulo_id}/itens", cursoApiHandlers.CreateItemModulo)
	r.Get("/modulos/{modulo_id}/itens", cursoApiHandlers.GetItensModulo)
	r.Get("/itensmodulo/{id}", cursoApiHandlers.GetItemModulo)
	r.Put("/itensmodulo/{id}", cursoApiHandlers.UpdateItemModulo)
	r.Delete("/itensmodulo/{id}", cursoApiHandlers.DeleteItemModulo)
	r.Post("/itensmodulo/{id}/mover", cursoApiHandlers.MoveItemModulo)

	r.Get("/alunocursos/{id}/itemmodulos", cursoApiHandlers.GetAlunoCursoItemModulos)
	r.Get("/alunocursoitemmodulos/{id}", cursoApiHandlers.GetAlunoCursoItemModulo)
	r.Patch("/alunocursoitemmodulos/{id}", cursoApiHandlers.UpdateAlunoCursoItemModulo)

	// Pessoas
	r.Get("/pessoas", cursoApiHandlers.GetPessoas)

	// Swagger
	swaggerDocsURL := fmt.Sprintf("http://localhost:%s/docs/doc.json", servicePort)
	r.Get("/docs/*", httpSwagger.Handler(httpSwagger.URL(swaggerDocsURL)))

	// Admin (QOR)
	r.Mount("/admin", adminPanel)

	return r
}
