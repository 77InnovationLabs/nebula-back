package kafka

import "context"

//go:generate mockgen -destination=../../mocks/mock_messaging.go -package=mocks fdqf01/curso/internal/infrastructure/messaging KafkaProducerInterface

type KafkaProducerInterface interface {
	PublishMessage(ctx context.Context, key, value string) error
	Close() error
}
