package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	domain_event "github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/event"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/event/handler"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/admin"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/api"
	database "github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/database/gorm"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/messaging"
	events_pkg "github.com/77InnovationLabs/nebula-back/pessoa/pkg/event_dispatcher"

	_ "github.com/77InnovationLabs/nebula-back/pessoa/docs"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth"
)

// @title           FDQF - Microserviço de Pessoas
// @version         0.0.1
// @description     Microserviço modelo para cadastro de pessoas
// @termsOfService  http://swagger.io/terms/

// @contact.name   Gustavo P Gialluisi
// @contact.url    http://www.fdqf.com
// @contact.email  atendimento@fdqf.com.br

// @license.name   FDQF License
// @license.url    http://license.fdqf.com.br

// @host      localhost:8081
// @BasePath  /
func main() {

	// Carregando variáveis de ambiente
	frontend_url := os.Getenv("FRONTEND_URL")
	log.Println("Frontend URL:", frontend_url)
	if frontend_url == "" {
		log.Println("FRONTEND_URL não configurada, usando valor padrão http://localhost:3000")
		frontend_url = "http://localhost:3000"
	}

	dbUser := os.Getenv("DB_PESSOA_USER")
	dbPassword := os.Getenv("DB_PESSOA_PASSWORD")
	dbName := os.Getenv("DB_PESSOA_NAME")
	dbHost := os.Getenv("DB_PESSOA_HOST")
	dbPort := os.Getenv("DB_PESSOA_PORT")

	service_port := os.Getenv("PESSOA_SERVICE_PORT")

	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	kafka_brokers_list := strings.Split(kafka_brokers, ",")

	// Inicialização do KAFKA
	err := startKafka(kafka_brokers)
	if err != nil {
		log.Fatalf("Erro ao inicializar Kafka: %v", err)
	}

	// String de conexão com o PostgreSQL
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		dbHost, dbUser, dbPassword, dbName, dbPort)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	//executa as migrações de todas entidades
	err = db.AutoMigrate(
		&entity.Pessoa{},
		&entity.Endereco{},
		&entity.Email{},
		&entity.Telefone{},
		&entity.User{},
	)
	if err != nil {
		log.Fatalf("failed to migrate database: %v", err)
	}

	//inicializar repositorios
	pessoaDB := database.NewPessoaRepositoryGorm(db)
	userDB := database.NewUserRepositoryGorm(db)

	//inicializa os eventos
	pessoa_changed_event := domain_event.NewPessoaChanged()

	//incializar event dispatchers
	eventDispatcher := events_pkg.NewEventDispatcher()
	eventDispatcher.Register(
		pessoa_changed_event.Name,
		&handler.PessoaChangedLogOnlyHandler{
			MsgPrefix: "INICIALIZANDO EVENTO PESSOA CHANGED",
		},
	)
	eventDispatcher.Register(
		pessoa_changed_event.Name,
		&handler.PessoaChangedKafkaHandler{
			KafkaProducer: messaging.NewKafkaProducer(kafka_brokers_list, "pessoa.saved"),
		},
	)

	//inicializa o jwtAuth
	tokenAuth := jwtauth.New("HS256", []byte(os.Getenv("JWT_SECRET")), nil)
	jwt_expires_in, err := strconv.Atoi(os.Getenv("JWT_EXPIRESIN"))
	if err != nil {
		log.Println("JWT_EXPIRESIN não configurado, usando valor padrão 300")
		jwt_expires_in = 300
	}

	//inicializar handlers
	pessoaApiHandlers := api.NewPessoaHandlers(
		eventDispatcher,
		pessoaDB,
		pessoa_changed_event,
	)
	userApiHandlers := api.NewUserHandlers(userDB, tokenAuth, jwt_expires_in)

	// Configurar o roteador Chi
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	// r.Use(middleware.Recoverer)
	r.Use(middleware.WithValue("jwt", tokenAuth))
	r.Use(middleware.WithValue("JwtExperesIn", jwt_expires_in))

	// Configurar CORS
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{frontend_url}, // ou []string{"*"} para permitir todas as origens (cuidado em produção)
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Tempo em segundos para cachear a preflight request
	}))

	//autenticação
	r.Post("/users", userApiHandlers.CreateUser)
	r.Post("/users/generate_token", userApiHandlers.GetJWT)

	//rotas do microserviço
	r.Post("/pessoas", pessoaApiHandlers.CreatePessoa)
	r.Post("/pessoas/v1", pessoaApiHandlers.CreatePessoaNomeEmail)
	r.Get("/pessoas", pessoaApiHandlers.GetPessoas)
	r.Get("/pessoas/{id}", pessoaApiHandlers.GetPessoa)
	r.Put("/pessoas/{id}", pessoaApiHandlers.UpdatePessoa)
	r.Delete("/pessoas/{id}", pessoaApiHandlers.DeletePessoa)

	r.Post("/enderecos", pessoaApiHandlers.CreateEndereco)
	r.Put("/enderecos/{id}", pessoaApiHandlers.UpdateEndereco)
	r.Get("/enderecos/{id}", pessoaApiHandlers.GetEndereco)
	r.Delete("/enderecos/{id}", pessoaApiHandlers.DeleteEndereco)
	r.Get("/pessoas/{parent}/enderecos", pessoaApiHandlers.GetEnderecosDaPessoa)

	r.Post("/emails", pessoaApiHandlers.CreateEmail)
	r.Put("/emails/{id}", pessoaApiHandlers.UpdateEmail)
	r.Get("/emails/{id}", pessoaApiHandlers.GetEmail)
	r.Delete("/emails/{id}", pessoaApiHandlers.DeleteEmail)
	r.Get("/pessoas/{parent}/emails", pessoaApiHandlers.GetEmailsDaPessoa)

	r.Post("/telefones", pessoaApiHandlers.CreateTelefone)
	r.Put("/telefones/{id}", pessoaApiHandlers.UpdateTelefone)
	r.Get("/telefones/{id}", pessoaApiHandlers.GetTelefone)
	r.Delete("/telefones/{id}", pessoaApiHandlers.DeleteTelefone)
	r.Get("/pessoas/{parent}/telefones", pessoaApiHandlers.GetTelefonesDaPessoa)

	// Configurar o swagger
	swagger_docs_url := fmt.Sprintf("http://localhost:%s/docs/doc.json", service_port)
	r.Get("/docs/*", httpSwagger.Handler(httpSwagger.URL(swagger_docs_url)))

	// Configurar o admin com qor5
	adminPanel := admin.InitializeAdmin(db)
	r.Mount("/admin", adminPanel)

	// Inicializar o servidor
	log.Default().Println("Iniciando servidor na porta: ", service_port)
	http.ListenAndServe(":"+service_port, r)
}
