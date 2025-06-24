# Usar a imagem oficial do Golang
FROM golang:1.24

# Criar diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar arquivos do projeto para dentro do contêiner
COPY . .

# Instalar dependências
RUN go mod tidy

# Compilar o binário do serviço
RUN go build -o eth-listener cmd/server/main.go

# Esperar o nó Geth iniciar antes de rodar o serviço
CMD ["sh", "-c", "sleep 10 && ./eth-listener"]
