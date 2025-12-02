package registry

import "time"

type RepositoryDTO struct {
	Id          string     `json:"id" db:"id"`
	Name        string     `json:"name" db:"name"`
	OwnerId     string     `json:"owner_id" db:"owner_id"`
	Path        string     `json:"path" db:"path"`
	IsPrivate   bool       `json:"is_private" db:"is_private"`
	Description *string    `json:"description,omitempty" db:"description"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at" db:"updated_at"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
}

type CollaboratorPermission string

const (
	PermissionRead  CollaboratorPermission = "read"
	PermissionWrite CollaboratorPermission = "write"
	PermissionAdmin CollaboratorPermission = "admin"
)

type RepositoryCollaboratorDTO struct {
	Id           string                 `json:"id" db:"id"`
	RepositoryId string                 `json:"repository_id" db:"repository_id"`
	UserId       string                 `json:"user_id" db:"user_id"`
	Permission   CollaboratorPermission `json:"permission" db:"permission"`
	CreatedAt    time.Time              `json:"created_at" db:"created_at"`
}
