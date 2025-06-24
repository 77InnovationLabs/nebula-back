package gorm

import (
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/entity"
	"github.com/77InnovationLabs/nebula-back/pessoa/internal/domain/repository"
	"gorm.io/gorm"
)

// Verifica se essa IMPLEMENTAÇÃO implementa corretamente a INTERFACE
var _ repository.UserRepositoryInterface = &UserRepositoryGorm{}

type UserRepositoryGorm struct {
	DB *gorm.DB
}

func NewUserRepositoryGorm(db *gorm.DB) *UserRepositoryGorm {
	return &UserRepositoryGorm{DB: db}
}

func (u *UserRepositoryGorm) Create(user *entity.User) error {
	return u.DB.Create(user).Error
}

func (u *UserRepositoryGorm) FindByEmail(email string) (*entity.User, error) {
	var user entity.User
	if err := u.DB.Where("email = ?", email).First(&user).Error; err != nil {
		return nil, err
	}
	return &user, nil
}
