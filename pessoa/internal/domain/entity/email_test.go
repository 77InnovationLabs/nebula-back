package entity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewEmail_ErrorIfEmptyID(t *testing.T) {
	obj := Email{}
	assert.Error(t, obj.IsValid(), "invalid id")
}

func TestNewEmail_ErrorIfEmptyEndereco(t *testing.T) {
	_, err := NewEmail(uuid.New(), nil, "", false)
	assert.Error(t, err, "invalid email")
}

func TestNewEmail_ErrorIfInvalidEndereco(t *testing.T) {
	_, err := NewEmail(uuid.New(), nil, "invalid-email", false)
	assert.Error(t, err, "invalid email")
}

func TestNewEmail_Success(t *testing.T) {
	obj, err := NewEmail(uuid.New(), nil, "some@email.com", false)
	assert.Nil(t, err)
	assert.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID)
	assert.Equal(t, "some@email.com", obj.Endereco)
	assert.False(t, obj.Principal)
}
