package repository

import (
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
)

type UserRepositoryInterface interface {
	Create(user *entity.User) error
	FindByEmail(email string) (*entity.User, error)
}
