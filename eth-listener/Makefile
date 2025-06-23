# Nome do binário do serviço
BINARY=eth-listener

# Caminho do diretório do código fonte
SRC=cmd/server/main.go

# Construção do binário localmente
build:
	@echo "🔨 Compilando o serviço..."
	go build -o $(BINARY) $(SRC)

# Limpeza dos arquivos compilados
clean:
	@echo "🧹 Limpando arquivos compilados..."
	rm -f $(BINARY)

# Rodar o serviço localmente (fora do Docker)
run:
	@echo "🚀 Rodando o serviço localmente..."
	go run $(SRC)

# Construção da imagem Docker
docker-build:
	@echo "🐳 Construindo a imagem Docker..."
	docker build -t $(BINARY) .

# Rodar o serviço no Docker individualmente
docker-run:
	@echo "🐳 Iniciando o contêiner Docker..."
	docker run --rm -p 8545:8545 --network=host $(BINARY)

# Subir tudo e iniciar o log
up-logs: up logs

# Subir toda a stack com Docker Compose
up:
	@echo "🚀 Subindo toda a stack (Kafka + Serviço)..."
	docker-compose up --build -d

# Down nos contêineres do projeto
down:
	@echo "🛑 Parando e removendo todos os serviços..."
	docker-compose down -v

# Stop nos contêineres do projeto
stop:
	@echo "🛑 Parando todos os serviços..."
	docker-compose stop

# Restart no contêiner informado por parametro
restart:
	@echo "🔄 Reiniciando o serviço..."
	docker-compose restart $(service)

# Ver logs do serviço Go
logs:
	@echo "📜 Logs do serviço..."
	docker-compose logs -f eth-listener

# log de um serviço específico informado por argumento
log-%:
	@echo "📜 Logs do serviço $*..."
	docker-compose logs -f $*

# Ver logs do Geth
logs-geth:
	@echo "📜 Logs do nó Geth..."
	docker-compose logs -f geth

# Ver logs do Kafka
logs-kafka:
	@echo "📜 Logs do Kafka..."
	docker-compose logs -f kafka

# Acessar o nó Geth via console
geth-console:
	@echo "🔧 Acessando console do Geth..."
	docker exec -it geth-node geth attach http://localhost:8545

# Remover contêineres e volumes
docker-reset:
	@echo "🗑️ Removendo contêineres e volumes..."
	docker-compose down -v
