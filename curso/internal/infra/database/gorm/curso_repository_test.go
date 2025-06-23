package gorm

import (
	"testing"

	"github.com/ggialluisi/fdqf-ms/curso/internal/domain/entity"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCreateNewCurso(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
	if err != nil {
		t.Error(err)
	}
	db.AutoMigrate(&entity.Pessoa{}, &entity.Curso{}, &entity.Modulo{})

	//cria a curso
	curso, err := entity.NewCurso(nil, "nome curso 1", "descricao 1")
	assert.NoError(t, err)

	cursoDB := NewCursoRepositoryGorm(db)
	curso, err = cursoDB.CreateCurso(curso)
	assert.NoError(t, err)
	assert.NotEmpty(t, curso.ID)
	assert.Equal(t, "nome curso 1", curso.Nome)
	assert.Equal(t, "descricao curso 1", curso.Descricao)
}

// func TestGetCursos(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Pessoa{}, &entity.Curso{}, &entity.Modulo{})

// 	//cria a pessoa
// 	pessoa_id := uuid.New()
// 	pessoa, err := entity.NewPessoa(&pessoa_id, "FISICA", "Nome Da Curso")
// 	assert.NoError(t, err)

// 	pessoaDB := NewPessoaRepositoryGorm(db)
// 	pessoa, err = pessoaDB.CreatePessoa(pessoa)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, pessoa.ID)

// 	//cria a curso
// 	data_contrato := time.Now()
// 	curso, err := entity.NewCurso(nil, pessoa.ID, "GESTAO", &data_contrato)
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	curso, err = cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, curso.ID)

// 	cursos, err := cursoDB.FindAllCursos(0, 0, "asc")
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, cursos)
// 	assert.Len(t, cursos, 1)
// 	assert.Equal(t, pessoa.ID, cursos[0].PessoaID)
// 	assert.Equal(t, pessoa.Nome, cursos[0].Nome())
// }
// func TestGetCursoByID(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Pessoa{}, &entity.Curso{}, &entity.Modulo{})

// 	//cria a pessoa
// 	pessoa_id := uuid.New()
// 	pessoa, err := entity.NewPessoa(&pessoa_id, "FISICA", "Nome Da Curso")
// 	assert.NoError(t, err)

// 	pessoaDB := NewPessoaRepositoryGorm(db)
// 	pessoa, err = pessoaDB.CreatePessoa(pessoa)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, pessoa.ID)

// 	//cria a curso
// 	data_contrato := time.Now()
// 	curso, err := entity.NewCurso(nil, pessoa.ID, "GESTAO", &data_contrato)
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	curso, err = cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, curso.ID)

// 	ret, err := cursoDB.GetCurso(curso.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret.ID)
// 	assert.Equal(t, pessoa.ID, ret.PessoaID)
// 	assert.Equal(t, "Nome Da Curso", ret.Nome())
// 	assert.Equal(t, entity.TipoAtuacao("GESTAO"), ret.TipoAtuacao)
// 	y, m, d := data_contrato.Date()
// 	assert.Equal(t, y, ret.DataContrato.Year())
// 	assert.Equal(t, m, ret.DataContrato.Month())
// 	assert.Equal(t, d, ret.DataContrato.Day())
// }

// func TestGetCursoByID(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// }

// func TestGetCursoByID_NotFound(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret, err := cursoDB.GetCurso(uuid.New())
// 	assert.Error(t, err)
// 	assert.Nil(t, ret)
// }

// func TestGetCursoByID_comEnderecos(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// }

// func TestGetCursoByID_comEnderecosModulosEmails(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	modulo1, err := entity.NewModulo(curso.ID, nil, "11", "123456789", true)
// 	assert.NoError(t, err)
// 	curso.Modulos = append(curso.Modulos, *modulo1)

// 	modulo2, err := entity.NewModulo(curso.ID, nil, "11", "987654321", false)
// 	assert.NoError(t, err)
// 	curso.Modulos = append(curso.Modulos, *modulo2)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// 	assert.Equal(t, len(ret1.Modulos), len(ret2.Modulos))
// 	assert.Equal(t, len(ret1.Modulos), 2)
// }

// func TestGetCursoByID_comEnderecosModulosEmails_ErrorIfOneNotformattedEmail(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	modulo1, err := entity.NewModulo(curso.ID, nil, "11", "123456789", true)
// 	assert.NoError(t, err)
// 	curso.Modulos = append(curso.Modulos, *modulo1)

// 	modulo2, err := entity.NewModulo(curso.ID, nil, "11", "987654321", false)
// 	assert.NoError(t, err)
// 	curso.Modulos = append(curso.Modulos, *modulo2)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// 	assert.Equal(t, len(ret1.Modulos), len(ret2.Modulos))
// 	assert.Equal(t, len(ret1.Modulos), 2)
// }

// func TestGetCursoByID_comEnderecosModulosEmails_ErrorIfOneNotformattedModulo(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	modulo1, err := entity.NewModulo(curso.ID, nil, "11", "123456789", true)
// 	assert.NoError(t, err)
// 	curso.Modulos = append(curso.Modulos, *modulo1)

// 	modulo2, err := entity.NewModulo(curso.ID, nil, "dd", "987654321", false)
// 	assert.Error(t, err)
// 	assert.Nil(t, modulo2)
// 	//assert error message
// 	assert.Equal(t, "invalid ddd", err.Error())

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// 	assert.Equal(t, len(ret1.Modulos), len(ret2.Modulos))
// 	assert.Equal(t, len(ret1.Modulos), 1)
// }

// func TestGetByDocumento(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCursoByDocumento("cpf da cursos")
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)
// }

// func TestDeleteCurso(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Pessoa{}, &entity.Curso{}, &entity.Modulo{})
// 	//cria a pessoa
// 	pessoa_id := uuid.New()
// 	pessoa, err := entity.NewPessoa(&pessoa_id, "FISICA", "Nome Da Curso")
// 	assert.NoError(t, err)

// 	pessoaDB := NewPessoaRepositoryGorm(db)
// 	pessoa, err = pessoaDB.CreatePessoa(pessoa)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, pessoa.ID)

// 	//cria a curso
// 	data_contrato := time.Now()
// 	curso, err := entity.NewCurso(nil, pessoa.ID, "GESTAO", &data_contrato)
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	curso, err = cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, curso.ID)

// 	err = cursoDB.DeleteCurso(curso.ID)
// 	assert.NoError(t, err)

// 	ret, err := cursoDB.GetCurso(curso.ID)
// 	assert.Error(t, err)
// 	assert.Nil(t, ret)
// }

// func TestDelete_comEndereco_verificaSeSenderecoFoiDeletado(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	curso, err := entity.NewCurso(nil, "FISICA", "Nome Da Curso", "cpf da cursos")
// 	assert.NoError(t, err)

// 	cursoDB := NewCursoRepositoryGorm(db)
// 	ret1, err := cursoDB.CreateCurso(curso)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret1.ID)

// 	ret2, err := cursoDB.GetCurso(ret1.ID)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, ret2.ID)
// 	assert.Equal(t, ret1.ID, ret2.ID)
// 	assert.Equal(t, ret1.TipoAtuacao, ret2.TipoAtuacao)
// 	assert.Equal(t, ret1.Nome, ret2.Nome)
// 	assert.Equal(t, ret1.Documento, ret2.Documento)

// 	err = cursoDB.DeleteCurso(ret1.ID)
// 	assert.NoError(t, err)

// 	ret3, err := cursoDB.GetCurso(ret1.ID)
// 	assert.Error(t, err)
// 	assert.Nil(t, ret3)
// }

// func TestFinalAllCursos(t *testing.T) {
// 	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{})
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	db.AutoMigrate(&entity.Curso{}, &entity.Modulo{})

// 	for i := 1; i < 24; i++ {
// 		item, err := entity.NewCurso(nil, "FISICA", fmt.Sprintf("Curso %d", i), fmt.Sprintf("cpf da curso %d", i))
// 		assert.NoError(t, err)
// 		db.Save(item)
// 	}

// 	repo := NewCursoRepositoryGorm(db)
// 	itens, err := repo.FindAllCursos(1, 10, "asc")
// 	assert.NoError(t, err)
// 	assert.Len(t, itens, 10)
// 	assert.Equal(t, "Curso 1", itens[0].Nome)
// 	assert.Equal(t, "Curso 10", itens[9].Nome)

// 	itens, err = repo.FindAllCursos(2, 10, "asc")
// 	assert.NoError(t, err)
// 	assert.Len(t, itens, 10)
// 	assert.Equal(t, "Curso 11", itens[0].Nome)
// 	assert.Equal(t, "Curso 20", itens[9].Nome)

// 	itens, err = repo.FindAllCursos(3, 10, "asc")
// 	assert.NoError(t, err)
// 	assert.Len(t, itens, 3)
// 	assert.Equal(t, "Curso 21", itens[0].Nome)
// 	assert.Equal(t, "Curso 23", itens[2].Nome)
// }
