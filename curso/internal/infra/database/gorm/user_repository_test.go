package gorm

import (
	"testing"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateNewUser(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("error connecting to database: %v", err)
	}
	db.AutoMigrate(&entity.User{})

	repo := NewUserRepositoryGorm(db)
	user, err := entity.NewUser("John Doe", "j@d.com", "123456")
	assert.NoError(t, err)

	err = repo.Create(user)
	assert.NoError(t, err)

	user2, err := repo.FindByEmail("j@d.com")
	assert.NoError(t, err)
	assert.Equal(t, user.Name, user2.Name)
	assert.Equal(t, user.Email, user2.Email)
	assert.Equal(t, user.Password, user2.Password)
	assert.NotEqual(t, user.Password, "123456")

	user3, err := repo.FindByEmail("xpto@x.com")
	assert.Error(t, err)
	assert.Nil(t, user3)
}
