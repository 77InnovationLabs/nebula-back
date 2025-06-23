package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/entity"
	"github.com/ggialluisi/fdqf-ms/pessoa/internal/domain/repository"
	"github.com/go-chi/jwtauth"
)

type Error struct {
	Message string `json:"message"`
}

type UserHandlers struct {
	UserDB       repository.UserRepositoryInterface
	Jwt          *jwtauth.JWTAuth
	JwtExpiresIn int
}

func NewUserHandlers(userDB repository.UserRepositoryInterface, jwt *jwtauth.JWTAuth, jwtExpiresIn int) *UserHandlers {
	return &UserHandlers{
		UserDB:       userDB,
		Jwt:          jwt,
		JwtExpiresIn: jwtExpiresIn,
	}
}

type GetJWTInput struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type GetJWTOutput struct {
	AccessToken string `json:"access_token"`
}

// GetJWT godoc
// @Summary      Get a user JWT
// @Description  Get a user JWT
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        request   body     string  true  "user credentials"
// @Success      200  {object}  string
// @Failure      404  {object}  string
// @Failure      500  {object}  string
// @Router       /users/generate_token [post]
func (h *UserHandlers) GetJWT(w http.ResponseWriter, r *http.Request) {
	// log.Default().Println("GetJWT 0")

	// jwt := r.Context().Value("jwt").(*jwtauth.JWTAuth)
	// log.Default().Println("GetJWT 1", jwt, "|")

	// jwtExpiresIn := r.Context().Value("JwtExperesIn").(int)
	// log.Default().Println("GetJWT 2", jwtExpiresIn, "|")

	var input_dto GetJWTInput
	err := json.NewDecoder(r.Body).Decode(&input_dto)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	u, err := h.UserDB.FindByEmail(input_dto.Email)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		err := Error{Message: err.Error()}
		json.NewEncoder(w).Encode(err)
		return
	}

	if !u.ValidatePassword(input_dto.Password) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	_, tokenString, _ := h.Jwt.Encode(map[string]interface{}{
		"sub": u.ID,
		"exp": time.Now().Add(time.Hour * time.Duration(h.JwtExpiresIn)).Unix(),
	})

	accessTokenSerializer := struct {
		AccessToken string `json:"access_token"`
	}{AccessToken: tokenString}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(accessTokenSerializer)
}

type CreateUserInput struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

// CreateUser godoc
// @Summary      Create user
// @Description  Create user
// @Tags         users
// @Accept       json
// @Produce      json
// @Param        request     body      string  true  "user request"
// @Success      201
// @Failure      500         {object}  Error
// @Router       /users [post]
func (h *UserHandlers) CreateUser(w http.ResponseWriter, r *http.Request) {
	var user CreateUserInput
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	u, err := entity.NewUser(user.Name, user.Email, user.Password)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		error := Error{Message: err.Error()}
		json.NewEncoder(w).Encode(error)
		return
	}
	err = h.UserDB.Create(u)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		error := Error{Message: err.Error()}
		json.NewEncoder(w).Encode(error)
		return
	}
	w.WriteHeader(http.StatusCreated)
}
