package repository

import (
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
)

type OrderRepositoryInterface interface {
	Save(order *entity.Order) error
	// GetTotal() (int, error)
}
