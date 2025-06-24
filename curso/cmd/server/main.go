package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/entity"
	domain_event "github.com/77InnovationLabs/nebula-back/curso/internal/domain/event"
	event_handler "github.com/77InnovationLabs/nebula-back/curso/internal/domain/event/handler"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/admin"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/api"
	database "github.com/77InnovationLabs/nebula-back/curso/internal/infra/database/gorm"
	msg_kafka "github.com/77InnovationLabs/nebula-back/curso/internal/infra/messaging/kafka"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/web"
	events_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"

	"github.com/IBM/sarama"
	"github.com/go-chi/jwtauth"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// @title           FDQF - Microserviço de Cursos
// @version         0.0.1
// @description     Microserviço modelo para cadastro de cursos
// @termsOfService  http://swagger.io/terms/

// @contact.name   Gustavo P Gialluisi
// @contact.url    http://www.fdqf.com
// @contact.email  atendimento@fdqf.com.br

// @license.name   FDQF License
// @license.url    http://license.fdqf.com.br

// @host      localhost:8083
// @BasePath  /
func main() {
	// Vars
	frontendURL := os.Getenv("FRONTEND_URL")
	if frontendURL == "" {
		frontendURL = "http://localhost:3000"
	}
	log.Println("✅ FRONTEND_URL:", frontendURL)

	dbUser := os.Getenv("DB_CURSO_USER")
	dbPassword := os.Getenv("DB_CURSO_PASSWORD")
	dbName := os.Getenv("DB_CURSO_NAME")
	dbHost := os.Getenv("DB_CURSO_HOST")
	dbPort := os.Getenv("DB_CURSO_PORT")
	servicePort := os.Getenv("CURSO_SERVICE_PORT")

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaBrokersList := strings.Split(kafkaBrokers, ",")

	// ✅ Inicializa Kafka (create topics) - apenas se LOCAL...
	if os.Getenv("ENV") == "LOCAL" {
		if err := ensureKafkaTopics(kafkaBrokersList); err != nil {
			log.Fatalf("Erro Kafka: %v", err)
		}
	} else {
		log.Println("🔑 Skip Kafka topic creation: production mode")
	}

	// ✅ PostgreSQL
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		dbHost, dbUser, dbPassword, dbName, dbPort)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Erro DB: %v", err)
	}

	if err := db.AutoMigrate(
		&entity.User{},
		&entity.Pessoa{},
		&entity.Curso{},
		&entity.Modulo{},
		&entity.Aluno{},
		&entity.AlunoCurso{},
		&entity.ItemModulo{},
		&entity.ItemModuloAula{},
		&entity.ItemModuloContractValidation{},
		&entity.AlunoCursoItemModulo{},
	); err != nil {
		log.Fatalf("Erro AutoMigrate: %v", err)
	}

	cursoDB := database.NewCursoRepositoryGorm(db)
	pessoaDB := database.NewPessoaRepositoryGorm(db)
	userDB := database.NewUserRepositoryGorm(db)

	// ✅ Sarama Consumer: define handlers
	pessoaHandler := msg_kafka.NewPessoaKafkaHandlers(pessoaDB)

	consumers := []*KafkaConsumer{
		{
			Topic:   "pessoa.saved",
			GroupID: "curso-group",
			Brokers: kafkaBrokersList,
			Handler: &SimpleHandler{
				handleFunc: func(msg *sarama.ConsumerMessage) error {
					return pessoaHandler.CreateOrUpdatePessoa(msg)
				},
			},
		},
	}

	go startKafkaConsumers(consumers)

	// ✅ Eventos + Producer
	eventDispatcher := events_pkg.NewEventDispatcher()

	producer := msg_kafka.NewKafkaProducer(kafkaBrokersList, "curso-saved", os.Getenv("KAFKA_KEY"), os.Getenv("KAFKA_SECRET"))

	cursoEvent := domain_event.NewCursoChanged()
	eventDispatcher.Register(cursoEvent.Name, &event_handler.CursoChangedKafkaHandler{
		KafkaProducer: producer,
	})

	// ✅ JWT
	tokenAuth := jwtauth.New("HS256", []byte(os.Getenv("JWT_SECRET")), nil)
	jwtExpiresIn, err := strconv.Atoi(os.Getenv("JWT_EXPIRESIN"))
	if err != nil {
		jwtExpiresIn = 300
	}

	// ✅ Handlers API
	cursoApiHandlers := api.NewCursoHandlers(
		eventDispatcher,
		cursoDB,
		cursoEvent,
		domain_event.NewModuloChanged(),
		domain_event.NewAlunoChanged(),
		domain_event.NewAlunoCursoChanged(),
		domain_event.NewItemModuloChanged(),
		pessoaDB,
	)
	userApiHandlers := api.NewUserHandlers(userDB, tokenAuth, jwtExpiresIn)

	// ✅ Admin
	adminPanel := admin.InitializeAdmin(db)

	// ✅ Router
	router := web.SetupRoutes(
		frontendURL,
		servicePort,
		tokenAuth,
		jwtExpiresIn,
		cursoApiHandlers,
		userApiHandlers,
		adminPanel,
	)

	log.Printf("🚀 Rodando na porta :%s", servicePort)
	http.ListenAndServe(":"+servicePort, router)
}
