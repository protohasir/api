package gitserver

import (
	"context"

	"hasir-api/internal/registry"
)

// RepositoryAdapter adapts internal registry repository to gitserver interface
type RepositoryAdapter struct {
	repo *registry.PgRepository
}

func NewRepositoryAdapter(repo *registry.PgRepository) *RepositoryAdapter {
	return &RepositoryAdapter{repo: repo}
}

func (a *RepositoryAdapter) GetRepositoryByPath(ctx context.Context, owner, name string) (interface{}, error) {
	return a.repo.GetRepositoryByPath(ctx, owner, name)
}

func (a *RepositoryAdapter) CheckRepositoryAccess(ctx context.Context, username, owner, repoName, accessType string) (bool, error) {
	return a.repo.CheckRepositoryAccess(ctx, username, owner, repoName, accessType)
}

func (a *RepositoryAdapter) CreatePhysicalRepository(ctx context.Context, owner, name, path string) error {
	// This is handled by the service layer
	return nil
}
