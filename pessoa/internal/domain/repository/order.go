package repository

import (
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/entity"
)

type OrderRepositoryInterface interface {
	Save(order *entity.Order) error
	// GetTotal() (int, error)
}
