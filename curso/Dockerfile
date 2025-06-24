# Etapa de build
FROM golang:1.24.4-alpine AS builder

# Instala dependências básicas
RUN apk add --no-cache git

# Define diretório de trabalho
WORKDIR /app

# Copia go mod e sum primeiro para cache de dependências
COPY go.mod go.sum ./
RUN go mod download

# Copia todo o restante do código
COPY . .

# Compila a aplicação para produção
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o curso-service ./cmd/server

# Etapa final: usa imagem base mínima
FROM alpine:latest

# Define diretório de trabalho na imagem final
WORKDIR /root/

# Copia o binário compilado da etapa de build
COPY --from=builder /app/curso-service .

# Expõe a porta (ajuste conforme sua variável de ambiente)
EXPOSE 8083

# Comando padrão para executar
CMD ["./curso-service"]
