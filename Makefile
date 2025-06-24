ARGS = $(filter-out $@,$(MAKECMDGOALS))

# Nome dos binários gerados
BINARY_CURSO=curso-service
BINARY_PESSOA=pessoa-service
BINARY_ETHLISTENER=eth-listener-service

# Diretórios principais
# main.go fica na raaiz do projeto, então não é necessário especificar o diretório
# PESSOA_DIR=cmd/server

# Definir comandos padrão
.PHONY: all build-pessoa run-pessoa test docker-build docker-run stop clean up createservice reset-docker-volumes tidy swag rename-entity logs

# Comando padrão para compilar todos os projetos
all: build-eth-listener build-pessoa build-curso
	@echo "Todos os microserviços foram compilados com sucesso."

# Compilar o microserviço Pessoa
build-pessoa:
	@echo "Compilando o microserviço Pessoa..."
	cd pessoa && go build -o $(BINARY_PESSOA) ./cmd/server/main.go

# Compilar o microserviço Curso
build-curso:
	@echo "Compilando o microserviço Curso..."
	cd curso && go build -o $(BINARY_CURSO) ./cmd/server/main.go

# Compilar o microserviço ETH-Listener
build-eth-listener:
	@echo "Compilando o microserviço eth-listener..."
	cd eth-listener && go build -o $(BINARY_ETHLISTENER) ./cmd/server/main.go

# Rodar o microserviço Pessoa localmente
run-pessoa:
	@echo "Rodando o microserviço Pessoa localmente..."
	cd pessoa && env $(cat .env | xargs) ./$(BINARY_PESSOA)

# Rodar o microserviço Curso localmente
run-curso:
	@echo "Rodando o microserviço Curso localmente..."
	cd curso && env $(cat .env | xargs) ./$(BINARY_CURSO)

# Executar testes para ambos os microserviços
test:
	@echo "Executando os testes..."
	cd pessoa && go test ./...
	cd curso && go test ./...

# Construir as imagens Docker para ambos os microserviços
docker-build:
	@echo "Construindo as imagens Docker..."
	docker-compose build

# Rodar o Docker Compose para todos os microserviços
docker-run:
	@echo "Subindo os containers com Docker Compose..."
	docker-compose up -d

# Parar e remover todos os containers
stop:
	@echo "Parando e removendo os containers..."
	docker-compose down

# Limpar arquivos binários para ambos os microserviços
clean:
	@echo "Limpando arquivos binários..."
	cd pessoa && go clean && rm -f $(BINARY_PESSOA)
	cd curso && go clean && rm -f $(BINARY_CURSO)

# Subir e executar todos os microserviços
up: docker-build docker-run
	@echo "Todos os microserviços foram construídos e estão em execução."

# Nome do alvo: reset-docker-volumes
reset-docker-volumes:
	@read -p "Isso vai resetar todas as bases de dados. Deseja continuar? (y/N) " confirm && \
	if [ "$$confirm" = "y" ]; then \
	  echo "Removendo volumes Docker..."; \
	  docker-compose down -v; \
	  echo "Reinicializando serviços..."; \
	  docker-compose up -d; \
	  echo "Volumes Docker foram removidos e serviços foram reinicializados."; \
	else \
	  echo "Operação cancelada."; \
	fi

# Limpar arquivos binários para ambos os microserviços
tidy:
	@echo "GO MOD TIDY geral..."
	cd pessoa && go mod tidy
	cd curso && go mod tidy
	cd eth-listener && go mod tidy

# Limpar arquivos binários para ambos os microserviços
mod-download:
	@echo "GO MOD DOWNLOAD geral..."
	cd pessoa && go mod download
	cd curso && go mod download
	cd eth-listener && go mod download

# Limpar arquivos binários para ambos os microserviços
swag:
	@echo "SWAGG INIT geral..."
	cd pessoa && swag init -g cmd/server/main.go
	cd curso && swag init -g cmd/server/main.go

createservice:
	@if [ -z "$(origem)" ] || [ -z "$(destino)" ]; then \
		echo "Erro: os parâmetros 'origem' e 'destino' são obrigatórios."; \
		exit 1; \
	fi

	@if [ ! -d "$(origem)" ]; then \
		echo "Erro: a pasta de origem '$(origem)' não foi encontrada."; \
		exit 1; \
	fi

	@echo "Criando o serviço '$(destino)' a partir de '$(origem)'..."

	# Copiando a pasta de origem para o destino
	@cp -r $(origem) $(destino)

	# Renomeando arquivos e diretórios que contenham o nome do serviço de origem
	@find $(destino) -name "*$(origem)*" | while read file; do \
		newfile=$$(echo $$file | sed "s/$(origem)/$(destino)/g"); \
		mv "$$file" "$$newfile"; \
	done

	# Substituindo o conteúdo dos arquivos de forma case-sensitive
	@find $(destino) -type f -exec sed -i \
		-e "s/$(shell echo $(origem) | awk '{print toupper($$0)}')/$(shell echo $(destino) | awk '{print toupper($$0)}')/g" \
		-e "s/$(shell echo $(origem) | awk '{print tolower($$0)}')/$(shell echo $(destino) | awk '{print tolower($$0)}')/g" \
		-e "s/$(shell echo $(origem) | awk '{print toupper(substr($$0,1,1))tolower(substr($$0,2))}')/$(shell echo $(destino) | awk '{print toupper(substr($$0,1,1))tolower(substr($$0,2))}')/g" \
		{} +

	@echo "Serviço '$(destino)' criado com sucesso."

rename-entity:
	@if [ -z "$(pasta)" ] || [ -z "$(texto-origem)" ] || [ -z "$(texto-destino)" ]; then \
		echo "Erro: os parâmetros 'pasta', 'texto-origem' e 'texto-destino' são obrigatórios."; \
		exit 1; \
	fi

	@if [ ! -d "$(pasta)" ]; then \
		echo "Erro: a pasta '$(pasta)' não foi encontrada."; \
		exit 1; \
	fi

	@echo "Renomeando a entidade de '$(texto-origem)' para '$(texto-destino)' na pasta '$(pasta)'..."

	# Renomeando arquivos e diretórios que contenham o nome da entidade de origem
	@find $(pasta) -name "*$(texto-origem)*" | while read file; do \
		newfile=$$(echo $$file | sed "s/$(texto-origem)/$(texto-destino)/g"); \
		mv "$$file" "$$newfile"; \
	done

	# Substituindo o conteúdo dos arquivos de forma case-sensitive
	@find $(pasta) -type f -exec sed -i \
		-e "s/$(shell echo $(texto-origem) | awk '{print toupper($$0)}')/$(shell echo $(texto-destino) | awk '{print toupper($$0)}')/g" \
		-e "s/$(shell echo $(texto-origem) | awk '{print tolower($$0)}')/$(shell echo $(texto-destino) | awk '{print tolower($$0)}')/g" \
		-e "s/$(shell echo $(texto-origem) | awk '{print toupper(substr($$0,1,1))tolower(substr($$0,2))}')/$(shell echo $(texto-destino) | awk '{print toupper(substr($$0,1,1))tolower(substr($$0,2))}')/g" \
		{} +

	@echo "Renomeação de entidade concluída com sucesso."

logs:
	docker-compose logs -f --tail 200 ${ARGS}

# Nome dos apps Heroku
PESSOA_APP=pessoa-service-app
CURSO_APP=curso-service-app

# Alvo para conectar no banco PESSOA
psql-pessoa:
	@echo "Abrindo psql para $(PESSOA_APP)..."
	heroku config:get DATABASE_URL --app $(PESSOA_APP) | xargs psql

# Alvo para conectar no banco CURSO
psql-curso:
	@echo "Abrindo psql para $(CURSO_APP)..."
	heroku config:get DATABASE_URL --app $(CURSO_APP) | xargs psql
