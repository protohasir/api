package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"hasir-api/pkg/config"
)

type Repository interface {
	CreateRepository(ctx context.Context, repo *RepositoryDTO) error
	GetRepositoryByName(ctx context.Context, name string) (*RepositoryDTO, error)
	GetRepositoryByPath(ctx context.Context, owner, name string) (*RepositoryDTO, error)
	GetRepositoryById(ctx context.Context, id string) (*RepositoryDTO, error)
	GetRepositories(ctx context.Context, page, pageSize int) (*[]RepositoryDTO, error)
	GetRepositoriesCount(ctx context.Context) (int, error)
	CheckRepositoryAccess(ctx context.Context, username, owner, repoName, accessType string) (bool, error)
	// Collaborator management
	AddCollaborator(ctx context.Context, collaborator *RepositoryCollaboratorDTO) error
	GetCollaborators(ctx context.Context, repoId string) ([]*RepositoryCollaboratorDTO, error)
	GetCollaboratorPermission(ctx context.Context, repoId, userId string) (*CollaboratorPermission, error)
	RemoveCollaborator(ctx context.Context, repoId, userId string) error
}

var (
	ErrFailedAcquireConnection = connect.NewError(connect.CodeInternal, errors.New("failed to acquire connection"))
	ErrRepositoryAlreadyExists = connect.NewError(connect.CodeAlreadyExists, errors.New("repository already exists"))
	ErrRepositoryNotFound      = connect.NewError(connect.CodeNotFound, errors.New("repository not found"))
	ErrUniqueViolationCode     = "23505"
)

type PgRepository struct {
	connectionPool *pgxpool.Pool
	tracer         trace.Tracer
}

func NewPgRepository(
	cfg *config.Config,
	traceProvider *sdktrace.TracerProvider,
) *PgRepository {
	credential := cfg.PostgresConfig.GetPostgresDsn()
	pgConfig, err := pgxpool.ParseConfig(credential)
	if err != nil {
		zap.L().Fatal("failed to parse database config", zap.Error(err))
	}

	if traceProvider != nil {
		pgConfig.ConnConfig.Tracer = otelpgx.NewTracer(
			otelpgx.WithTracerProvider(traceProvider),
			otelpgx.WithDisableConnectionDetailsInAttributes(),
		)
	}

	var pgConnectionPool *pgxpool.Pool
	pgConnectionPool, err = pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		zap.L().Fatal("failed to connect database", zap.Error(err))
	}

	if traceProvider != nil {
		if err := otelpgx.RecordStats(pgConnectionPool); err != nil {
			zap.L().Fatal("unable to record database stats", zap.Error(err))
		}
	}

	var connection *pgxpool.Conn
	connection, err = pgConnectionPool.Acquire(context.Background())
	if err != nil {
		zap.L().Fatal("failed to acquire connection", zap.Error(err))
	}
	defer connection.Release()

	err = connection.Ping(context.Background())
	if err != nil {
		zap.L().Fatal("failed to ping database", zap.Error(err))
	}

	var tracer trace.Tracer
	if traceProvider != nil {
		tracer = traceProvider.Tracer("RepositoryPostgreSQLRepository")
	} else {
		tracer = noop.NewTracerProvider().Tracer("RepositoryPostgreSQLRepository")
	}

	return &PgRepository{
		connectionPool: pgConnectionPool,
		tracer:         tracer,
	}
}

func (r *PgRepository) CreateRepository(ctx context.Context, repo *RepositoryDTO) error {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "CreateRepository", trace.WithAttributes(attribute.KeyValue{
		Key:   "newRepository",
		Value: attribute.StringValue(fmt.Sprintf("%+v", repo)),
	}))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := `INSERT INTO repositories (id, name, owner_id, path, created_at, updated_at) 
			VALUES (@Id, @Name, @OwnerId, @Path, @CreatedAt, @UpdatedAt)`
	sqlArgs := pgx.NamedArgs{
		"Id":        repo.Id,
		"Name":      repo.Name,
		"OwnerId":   repo.OwnerId,
		"Path":      repo.Path,
		"CreatedAt": time.Now().UTC(),
		"UpdatedAt": time.Now().UTC(),
	}

	if _, err = connection.Exec(ctx, sql, sqlArgs); err != nil {
		span.RecordError(err)

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == ErrUniqueViolationCode {
				return ErrRepositoryAlreadyExists
			}
			return err
		}

		return connect.NewError(connect.CodeInternal, errors.New("failed to execute insert repository query"))
	}

	return nil
}

func (r *PgRepository) GetRepositoryByName(ctx context.Context, name string) (*RepositoryDTO, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetRepositoryByName", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "name",
			Value: attribute.StringValue(name),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "SELECT * FROM repositories WHERE name = $1 AND deleted_at IS NULL"

	var rows pgx.Rows
	rows, err = connection.Query(ctx, sql, name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("something went wrong"))
	}
	defer rows.Close()

	var repo RepositoryDTO
	repo, err = pgx.CollectOneRow[RepositoryDTO](rows, pgx.RowToStructByName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRepositoryNotFound
		}

		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to collect row"))
	}

	return &repo, nil
}

func (r *PgRepository) GetRepositoryByPath(ctx context.Context, owner, name string) (*RepositoryDTO, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetRepositoryByPath", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "owner",
			Value: attribute.StringValue(owner),
		},
		attribute.KeyValue{
			Key:   "name",
			Value: attribute.StringValue(name),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	// For now, just match by name. In production, you'd join with users table
	// and match owner username or organization name
	sql := "SELECT * FROM repositories WHERE name = $1 AND deleted_at IS NULL"

	var rows pgx.Rows
	rows, err = connection.Query(ctx, sql, name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("something went wrong"))
	}
	defer rows.Close()

	var repo RepositoryDTO
	repo, err = pgx.CollectOneRow[RepositoryDTO](rows, pgx.RowToStructByName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRepositoryNotFound
		}

		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to collect row"))
	}

	return &repo, nil
}

func (r *PgRepository) GetRepositoryById(ctx context.Context, id string) (*RepositoryDTO, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetRepositoryById", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "id",
			Value: attribute.StringValue(id),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "SELECT * FROM repositories WHERE id = $1 AND deleted_at IS NULL"

	rows, err := connection.Query(ctx, sql, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("something went wrong"))
	}
	defer rows.Close()

	repo, err := pgx.CollectOneRow[RepositoryDTO](rows, pgx.RowToStructByName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRepositoryNotFound
		}
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to collect row"))
	}

	return &repo, nil
}

func (r *PgRepository) CheckRepositoryAccess(ctx context.Context, username, owner, repoName, accessType string) (bool, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "CheckRepositoryAccess", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "username",
			Value: attribute.StringValue(username),
		},
		attribute.KeyValue{
			Key:   "owner",
			Value: attribute.StringValue(owner),
		},
		attribute.KeyValue{
			Key:   "repoName",
			Value: attribute.StringValue(repoName),
		},
		attribute.KeyValue{
			Key:   "accessType",
			Value: attribute.StringValue(accessType),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer connection.Release()

	// Get repository
	repo, err := r.GetRepositoryByPath(ctx, owner, repoName)
	if err != nil {
		return false, err
	}

	// If repository is public and access is read, allow
	if !repo.IsPrivate && accessType == "read" {
		return true, nil
	}

	// Get user ID by username
	var userId string
	userSql := "SELECT id FROM users WHERE username = $1 AND deleted_at IS NULL"
	err = connection.QueryRow(ctx, userSql, username).Scan(&userId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	// Check if user is owner
	if repo.OwnerId == userId {
		return true, nil
	}

	// Check collaborator permissions
	permission, err := r.GetCollaboratorPermission(ctx, repo.Id, userId)
	if err != nil {
		// Not a collaborator
		return false, nil
	}

	// Check permission level
	switch accessType {
	case "read":
		return *permission == PermissionRead || *permission == PermissionWrite || *permission == PermissionAdmin, nil
	case "write":
		return *permission == PermissionWrite || *permission == PermissionAdmin, nil
	case "admin":
		return *permission == PermissionAdmin, nil
	default:
		return false, nil
	}
}

func (r *PgRepository) GetRepositories(ctx context.Context, page, pageSize int) (*[]RepositoryDTO, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetRepositories", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "page",
			Value: attribute.IntValue(page),
		},
		attribute.KeyValue{
			Key:   "pageSize",
			Value: attribute.IntValue(pageSize),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	offset := (page - 1) * pageSize
	sql := "SELECT * FROM repositories WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT $1 OFFSET $2"

	var rows pgx.Rows
	rows, err = connection.Query(ctx, sql, pageSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to query repositories"))
	}
	defer rows.Close()

	repos, err := pgx.CollectRows[RepositoryDTO](rows, pgx.RowToStructByName)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to collect rows"))
	}

	return &repos, nil
}

func (r *PgRepository) GetRepositoriesCount(ctx context.Context) (int, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetRepositoriesCount")
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return 0, ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "SELECT COUNT(*) FROM repositories WHERE deleted_at IS NULL"

	var count int
	err = connection.QueryRow(ctx, sql).Scan(&count)
	if err != nil {
		span.RecordError(err)
		return 0, connect.NewError(connect.CodeInternal, errors.New("failed to count repositories"))
	}

	return count, nil
}

func (r *PgRepository) AddCollaborator(ctx context.Context, collaborator *RepositoryCollaboratorDTO) error {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "AddCollaborator", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "repositoryId",
			Value: attribute.StringValue(collaborator.RepositoryId),
		},
		attribute.KeyValue{
			Key:   "userId",
			Value: attribute.StringValue(collaborator.UserId),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := `INSERT INTO repository_collaborators (id, repository_id, user_id, permission, created_at) 
			VALUES (@Id, @RepositoryId, @UserId, @Permission, @CreatedAt)`
	sqlArgs := pgx.NamedArgs{
		"Id":           collaborator.Id,
		"RepositoryId": collaborator.RepositoryId,
		"UserId":       collaborator.UserId,
		"Permission":   collaborator.Permission,
		"CreatedAt":    time.Now().UTC(),
	}

	if _, err = connection.Exec(ctx, sql, sqlArgs); err != nil {
		span.RecordError(err)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == ErrUniqueViolationCode {
				return connect.NewError(connect.CodeAlreadyExists, errors.New("collaborator already exists"))
			}
		}
		return connect.NewError(connect.CodeInternal, errors.New("failed to add collaborator"))
	}

	return nil
}

func (r *PgRepository) GetCollaborators(ctx context.Context, repoId string) ([]*RepositoryCollaboratorDTO, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetCollaborators", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "repositoryId",
			Value: attribute.StringValue(repoId),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "SELECT * FROM repository_collaborators WHERE repository_id = $1 ORDER BY created_at DESC"

	rows, err := connection.Query(ctx, sql, repoId)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to query collaborators"))
	}
	defer rows.Close()

	collaborators, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[RepositoryCollaboratorDTO])
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to collect collaborators"))
	}

	return collaborators, nil
}

func (r *PgRepository) GetCollaboratorPermission(ctx context.Context, repoId, userId string) (*CollaboratorPermission, error) {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "GetCollaboratorPermission", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "repositoryId",
			Value: attribute.StringValue(repoId),
		},
		attribute.KeyValue{
			Key:   "userId",
			Value: attribute.StringValue(userId),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "SELECT permission FROM repository_collaborators WHERE repository_id = $1 AND user_id = $2"

	var permission CollaboratorPermission
	err = connection.QueryRow(ctx, sql, repoId, userId).Scan(&permission)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("collaborator not found"))
		}
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to get permission"))
	}

	return &permission, nil
}

func (r *PgRepository) RemoveCollaborator(ctx context.Context, repoId, userId string) error {
	var span trace.Span
	ctx, span = r.tracer.Start(ctx, "RemoveCollaborator", trace.WithAttributes(
		attribute.KeyValue{
			Key:   "repositoryId",
			Value: attribute.StringValue(repoId),
		},
		attribute.KeyValue{
			Key:   "userId",
			Value: attribute.StringValue(userId),
		},
	))
	defer span.End()

	connection, err := r.connectionPool.Acquire(ctx)
	if err != nil {
		return ErrFailedAcquireConnection
	}
	defer connection.Release()

	sql := "DELETE FROM repository_collaborators WHERE repository_id = $1 AND user_id = $2"
	result, err := connection.Exec(ctx, sql, repoId, userId)
	if err != nil {
		span.RecordError(err)
		return connect.NewError(connect.CodeInternal, errors.New("failed to remove collaborator"))
	}

	if result.RowsAffected() == 0 {
		return connect.NewError(connect.CodeNotFound, errors.New("collaborator not found"))
	}

	return nil
}
