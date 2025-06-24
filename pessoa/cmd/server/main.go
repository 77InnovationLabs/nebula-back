package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	domain_event "github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/event"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/event/handler"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/admin"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/api"
	database "github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/database/gorm"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/infra/messaging"
	events_pkg "github.com/77InnovationLabs/nebula-back/pessoa/pkg/event_dispatcher"

	"github.com/IBM/sarama"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	_ "github.com/77InnovationLabs/nebula-back/pessoa/docs"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth"
)

func main() {
	// ✅ Vars
	frontendURL := os.Getenv("FRONTEND_URL")
	if frontendURL == "" {
		frontendURL = "http://localhost:3000"
	}
	log.Println("✅ FRONTEND_URL:", frontendURL)

	db_sslmode := os.Getenv("DB_SSLMODE")
	dbUser := os.Getenv("DB_PESSOA_USER")
	dbPassword := os.Getenv("DB_PESSOA_PASSWORD")
	dbName := os.Getenv("DB_PESSOA_NAME")
	dbHost := os.Getenv("DB_PESSOA_HOST")
	dbPort := os.Getenv("DB_PESSOA_PORT")
	servicePort := os.Getenv("PESSOA_SERVICE_PORT")

	kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	kafkaUser := os.Getenv("KAFKA_KEY")
	kafkaPass := os.Getenv("KAFKA_SECRET")

	// ✅ PostgreSQL
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s",
		dbHost, dbUser, dbPassword, dbName, dbPort, db_sslmode)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("❌ Erro DB: %v", err)
	}

	if err := db.AutoMigrate(
		&entity.Pessoa{},
		&entity.Endereco{},
		&entity.Email{},
		&entity.Telefone{},
		&entity.User{},
	); err != nil {
		log.Fatalf("❌ Erro AutoMigrate: %v", err)
	}

	pessoaDB := database.NewPessoaRepositoryGorm(db)
	userDB := database.NewUserRepositoryGorm(db)

	// ✅ Configura Sarama Producer Global
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Net.MaxOpenRequests = 1

	// SASL/SSL Confluent
	if os.Getenv("ENV") == "LOCAL" {
		saramaConfig.Net.SASL.Enable = false
		saramaConfig.Net.TLS.Enable = false
	} else {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = kafkaUser
		saramaConfig.Net.SASL.Password = kafkaPass
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		saramaConfig.Net.TLS.Enable = true
	}
	saramaConfig.Version = sarama.V2_5_0_0

	producer, err := sarama.NewSyncProducer(kafkaBrokers, saramaConfig)
	if err != nil {
		log.Fatalf("❌ Erro ao criar Sarama Producer: %v", err)
	}
	defer producer.Close()

	log.Println("✅ Sarama Producer OK!")

	// ✅ Eventos + Dispatcher
	eventDispatcher := events_pkg.NewEventDispatcher()
	pessoaEvent := domain_event.NewPessoaChanged()

	eventDispatcher.Register(
		pessoaEvent.Name,
		&handler.PessoaChangedLogOnlyHandler{
			MsgPrefix: "📢 PESSOA CHANGED LOG",
		},
	)

	eventDispatcher.Register(
		pessoaEvent.Name,
		&handler.PessoaChangedKafkaHandler{
			KafkaProducer: messaging.NewKafkaProducer(producer, "pessoa.saved"),
		},
	)

	// ✅ JWT
	tokenAuth := jwtauth.New("HS256", []byte(os.Getenv("JWT_SECRET")), nil)
	jwtExpiresIn, err := strconv.Atoi(os.Getenv("JWT_EXPIRESIN"))
	if err != nil {
		log.Println("JWT_EXPIRESIN não configurado, usando padrão 300")
		jwtExpiresIn = 300
	}

	// ✅ Handlers
	pessoaApiHandlers := api.NewPessoaHandlers(eventDispatcher, pessoaDB, pessoaEvent)
	userApiHandlers := api.NewUserHandlers(userDB, tokenAuth, jwtExpiresIn)

	// ✅ Router
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.WithValue("jwt", tokenAuth))
	r.Use(middleware.WithValue("JwtExperesIn", jwtExpiresIn))
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{frontendURL},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// ✅ Rotas
	r.Post("/users", userApiHandlers.CreateUser)
	r.Post("/users/generate_token", userApiHandlers.GetJWT)

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

	// ✅ Swagger
	swaggerURL := fmt.Sprintf("http://localhost:%s/docs/doc.json", servicePort)
	r.Get("/docs/*", httpSwagger.Handler(httpSwagger.URL(swaggerURL)))

	// ✅ Admin com Qor5
	adminPanel := admin.InitializeAdmin(db)
	r.Mount("/admin", adminPanel)

	// ✅ Iniciar servidor
	log.Printf("🚀 Pessoa Service rodando na porta :%s", servicePort)
	http.ListenAndServe(":"+servicePort, r)
}
