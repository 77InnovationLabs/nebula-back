package messaging

import "context"

//go:generate mockgen -destination=../../mocks/mock_messaging.go -package=mocks fdqf01/pessoa/internal/infrastructure/messaging KafkaProducerInterface

type KafkaProducerInterface interface {
	PublishMessage(ctx context.Context, key, value string) error
}
