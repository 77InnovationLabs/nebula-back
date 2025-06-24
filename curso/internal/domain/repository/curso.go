package repository

import (
	"github.com/77InnovationLabs/nebula-back/curso/internal/domain/entity"
	"github.com/google/uuid"
)

type CursoRepositoryInterface interface {
	CreateCurso(obj *entity.Curso) (*entity.Curso, error)
	UpdateCurso(obj *entity.Curso) (*entity.Curso, error)
	DeleteCurso(objID uuid.UUID) error
	GetCurso(objID uuid.UUID) (*entity.Curso, error)
	GetCursoByDocumento(documento string) (*entity.Curso, error)
	FindAllCursos(page, limit int, sort string) ([]entity.Curso, error)

	CreateModulo(obj *entity.Modulo) (*entity.Modulo, error)
	UpdateModulo(obj *entity.Modulo) (*entity.Modulo, error)
	DeleteModulo(objID uuid.UUID) error
	GetModulo(objID uuid.UUID) (*entity.Modulo, error)
	GetModulosDeCurso(parentID uuid.UUID) ([]entity.Modulo, error)

	CreateItemModulo(item *entity.ItemModulo) error
	FindItemModuloByID(id uuid.UUID) (*entity.ItemModulo, error)
	FindItemModulosByModulo(moduloID uuid.UUID) ([]entity.ItemModulo, error)
	UpdateItemModulo(item *entity.ItemModulo) error
	DeleteItemModulo(id uuid.UUID) error
	MoveItemModulo(id uuid.UUID, action string) error
	GetMaxOrdemItemModulo(moduloID uuid.UUID) (int, error)

	CreateAluno(obj *entity.Aluno) (*entity.Aluno, error)
	UpdateAluno(obj *entity.Aluno) (*entity.Aluno, error)
	DeleteAluno(objID uuid.UUID) error
	GetAluno(objID uuid.UUID) (*entity.Aluno, error)
	GetAlunoByWallet(wallet string) (*entity.Aluno, error)
	GetAlunoByDocumento(documento string) (*entity.Aluno, error)
	FindAllAlunos(page, limit int, sort string) ([]entity.Aluno, error)
	HasAlunoPagamentoPendente(alunoID uuid.UUID) (bool, error)

	CreateAlunoCurso(obj *entity.AlunoCurso) (*entity.AlunoCurso, error)
	UpdateAlunoCurso(obj *entity.AlunoCurso) (*entity.AlunoCurso, error)
	DeleteAlunoCurso(objID uuid.UUID) error
	GetAlunoCurso(objID uuid.UUID) (*entity.AlunoCurso, error)
	FindAllAlunoCursos(page, limit int, sort string) ([]entity.AlunoCurso, error)
	FindCursosDoAluno(alunoID uuid.UUID) ([]entity.AlunoCurso, error)
	FindAlunosDoCurso(cursoID uuid.UUID) ([]entity.AlunoCurso, error)
	CountCursosDoAluno(alunoID uuid.UUID) (int64, error)
	CountAlunosDoCurso(cursoID uuid.UUID) (int64, error)

	CreateAlunoCursoItemModulosBatch(items []*entity.AlunoCursoItemModulo) error
	FindItemModulosByAlunoCurso(alunoCursoID uuid.UUID) ([]entity.AlunoCursoItemModulo, error)
	GetAlunoCursoItemModulo(id uuid.UUID) (*entity.AlunoCursoItemModulo, error)
	UpdateAlunoCursoItemModulo(item *entity.AlunoCursoItemModulo) error
}
