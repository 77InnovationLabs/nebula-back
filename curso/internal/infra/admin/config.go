package admin

import (
	"net/http"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"

	"github.com/qor5/admin/presets"
	"github.com/qor5/admin/presets/gorm2op"
	"github.com/qor5/ui/vuetify"
	"github.com/qor5/web"
	h "github.com/theplant/htmlgo"
	"gorm.io/gorm"
)

func InitializeAdmin(db *gorm.DB) *http.ServeMux {
	b := initializeProject(db)
	mux := SetupRouter(b)

	return mux
}

func initializeProject(db *gorm.DB) (b *presets.Builder) {
	// Initialize the builder of QOR5
	b = presets.New()

	// Set up the project name, ORM and Homepage
	b.URIPrefix("/admin").
		BrandTitle("Curso").
		DataOperator(gorm2op.DataOperator(db)).
		HomePageFunc(func(ctx *web.EventContext) (r web.PageResponse, err error) {
			r.Body = vuetify.VContainer(
				h.H1("Home"),
				h.P().Text("Change your home page here"))
			return
		})

	// register all models
	setupUserAdmin(b)
	setupCursoAdmin(b)
	setupModuloAdmin(b)
	setupPessoaAdmin(b)
	setupAlunoAdmin(b)
	setupAlunoCursoAdmin(b)
	setupItemModuloAdmin(b)
	setupAlunoCursoItemModuloAdmin(b)

	return b
}

func setupUserAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	// to-do... exemplos: https://github.com/qor5/admin/blob/main/example/admin/category_config.go#L15
	m := b.Model(&entity.User{})
	_ = m
}

func setupCursoAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.Curso{})
	_ = m
}

func setupModuloAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.Modulo{})
	_ = m
}

func setupPessoaAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.Pessoa{})
	_ = m
}

func setupAlunoAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.Aluno{})
	_ = m
}

func setupAlunoCursoAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.AlunoCurso{})
	_ = m
}

func setupItemModuloAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.ItemModulo{})
	_ = m
}

func setupAlunoCursoItemModuloAdmin(b *presets.Builder) {
	// Register model into the builder
	// Use m to customize the model
	m := b.Model(&entity.AlunoCursoItemModulo{})
	_ = m
}
