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

	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/entity"
	domain_event "github.com/77InnovationLabs/nebula-back/curso/internal/domain/event"
	event_handler "github.com/77InnovationLabs/nebula-back/curso/internal/domain/event/handler"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/admin"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/api"
	database "github.com/77InnovationLabs/nebula-back/curso/internal/infra/database/gorm"
	msg_kafka "github.com/77InnovationLabs/nebula-back/curso/internal/infra/messaging/kafka"
	"github.com/77InnovationLabs/nebula-back/curso/internal/infra/web"
	events_pkg "github.com/77InnovationLabs/nebula-back/curso/pkg/event_dispatcher"
	"github.com/segmentio/kafka-go"

	_ "github.com/77InnovationLabs/nebula-back/curso/docs"

	"github.com/go-chi/jwtauth"
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

// @host      localhost:8082
// @BasePath  /
func main() {

	// Carregando variáveis de ambiente
	frontend_url := os.Getenv("FRONTEND_URL")
	log.Println("Frontend URL:", frontend_url)
	if frontend_url == "" {
		log.Println("FRONTEND_URL não configurada, usando valor padrão http://localhost:3000")
		frontend_url = "http://localhost:3000"
	}

	dbUser := os.Getenv("DB_CURSO_USER")
	dbPassword := os.Getenv("DB_CURSO_PASSWORD")
	dbName := os.Getenv("DB_CURSO_NAME")
	dbHost := os.Getenv("DB_CURSO_HOST")
	dbPort := os.Getenv("DB_CURSO_PORT")

	service_port := os.Getenv("CURSO_SERVICE_PORT")

	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	kafka_brokers_list := strings.Split(kafka_brokers, ",")

	// Inicialização do KAFKA
	err := startKafka(
		kafka_brokers,
	)
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
	)
	if err != nil {
		log.Fatalf("failed to migrate database: %v", err)
	}

	//inicializar repositorios de cada agregado
	cursoDB := database.NewCursoRepositoryGorm(db)
	pessoaDB := database.NewPessoaRepositoryGorm(db)
	userDB := database.NewUserRepositoryGorm(db)

	// Inicializar os handlers do consumidor Kafka
	pessoaChangedHandler := msg_kafka.NewPessoaKafkaHandlers(pessoaDB)
	// userChangedHandler := msg_kafka.NewUserKafkaHandlers(userDB)

	// Criar uma lista de consumidores Kafka, com handlers personalizados
	consumers := []*KafkaConsumer{
		{
			topic:     "pessoa.saved",
			partition: 0,
			brokers:   kafka_brokers_list,
			handler: func(msg kafka.Message) error {
				return pessoaChangedHandler.CreateOrUpdatePessoa(msg)
			},
		},
	}

	// Executar os consumidores Kafka em uma goroutine
	go startKafkaConsumers(consumers)

	//inicializa os eventos
	curso_changed_event := domain_event.NewCursoChanged()
	modulo_changed_event := domain_event.NewModuloChanged()
	aluno_changed_event := domain_event.NewAlunoChanged()
	aluno_curso_changed_event := domain_event.NewAlunoCursoChanged()
	item_modulo_changed_event := domain_event.NewItemModuloChanged()

	//incializar event dispatchers
	eventDispatcher := events_pkg.NewEventDispatcher()
	eventDispatcher.Register(
		curso_changed_event.Name,
		&event_handler.CursoChangedKafkaHandler{
			KafkaProducer: msg_kafka.NewKafkaProducer(kafka_brokers_list, "curso.saved"),
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
	cursoApiHandlers := api.NewCursoHandlers(
		eventDispatcher,
		cursoDB,
		curso_changed_event,
		modulo_changed_event,
		aluno_changed_event,
		aluno_curso_changed_event,
		item_modulo_changed_event,
		pessoaDB,
	)
	userApiHandlers := api.NewUserHandlers(
		userDB,
		tokenAuth,
		jwt_expires_in,
	)

	// Configurar o admin com qor5
	adminPanel := admin.InitializeAdmin(db)

	// Setup do router
	router := web.SetupRoutes(
		frontend_url,
		service_port,
		tokenAuth,
		jwt_expires_in,
		cursoApiHandlers,
		userApiHandlers,
		adminPanel,
	)

	// Inicializar o servidor
	log.Printf("Iniciando servidor na porta: %s", service_port)
	http.ListenAndServe(":"+service_port, router)
}
