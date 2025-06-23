package repository

import (
	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"
)

type UserRepositoryInterface interface {
	Create(user *entity.User) error
	FindByEmail(email string) (*entity.User, error)
}
