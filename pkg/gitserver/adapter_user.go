package gitserver

import (
	"context"

	"hasir-api/internal/user"
)

// UserAdapter adapts internal user repository to gitserver interface
type UserAdapter struct {
	repo *user.PgRepository
}

func NewUserAdapter(repo *user.PgRepository) *UserAdapter {
	return &UserAdapter{repo: repo}
}

func (a *UserAdapter) GetUserByUsername(ctx context.Context, username string) (interface{}, error) {
	return a.repo.GetUserByUsername(ctx, username)
}

func (a *UserAdapter) ValidateUserPassword(ctx context.Context, username, password string) (bool, error) {
	return a.repo.ValidateUserPassword(ctx, username, password)
}

func (a *UserAdapter) GetUserPublicKeys(ctx context.Context, username string) ([]string, error) {
	return a.repo.GetUserPublicKeys(ctx, username)
}
