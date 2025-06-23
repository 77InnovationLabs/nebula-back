package repository

import (
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/entity"
)

type UserRepositoryInterface interface {
	Create(user *entity.User) error
	FindByEmail(email string) (*entity.User, error)
}
