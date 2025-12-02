package gitserver

import "context"

// UserRepository defines methods for user-related operations
type UserRepository interface {
	// GetUserByUsername retrieves a user by username
	GetUserByUsername(ctx context.Context, username string) (interface{}, error)

	// ValidateUserPassword validates user credentials
	ValidateUserPassword(ctx context.Context, username, password string) (bool, error)

	// GetUserPublicKeys retrieves user's SSH public keys
	GetUserPublicKeys(ctx context.Context, username string) ([]string, error)
}

// RepositoryRepository defines methods for repository-related operations
type RepositoryRepository interface {
	// GetRepositoryByPath retrieves a repository by owner and name
	GetRepositoryByPath(ctx context.Context, owner, name string) (interface{}, error)

	// CheckRepositoryAccess checks if user has access to repository
	CheckRepositoryAccess(ctx context.Context, username, owner, repoName, accessType string) (bool, error)

	// CreatePhysicalRepository creates the physical git repository on disk
	CreatePhysicalRepository(ctx context.Context, owner, name, path string) error
}
