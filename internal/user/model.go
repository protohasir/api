package user

import "time"

type UserDTO struct {
	Id        string     `json:"id" db:"id"`
	Username  string     `json:"username" db:"username"`
	Email     string     `json:"email" db:"email"`
	Password  string     `json:"password" db:"password"`
	CreatedAt time.Time  `json:"created_at" db:"created_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
}

type RefreshTokensDTO struct {
	UserId    string    `json:"id" db:"id"`
	Token     string    `json:"token" db:"token"`
	ExpiresAt time.Time `json:"expires_at" db:"expires_at"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type UserSSHKeyDTO struct {
	Id          string     `json:"id" db:"id"`
	UserId      string     `json:"user_id" db:"user_id"`
	Title       string     `json:"title" db:"title"`
	PublicKey   string     `json:"public_key" db:"public_key"`
	Fingerprint string     `json:"fingerprint" db:"fingerprint"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty" db:"last_used_at"`
}
