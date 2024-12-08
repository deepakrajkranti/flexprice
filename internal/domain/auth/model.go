package auth

import (
	"time"

	"github.com/flexprice/flexprice/internal/types"
)

type Auth struct {
	UserID    string             `db:"user_id" json:"user_id"` // unique identifier for this table
	Provider  types.AuthProvider `db:"provider" json:"provider"`
	Token     string             `db:"token" json:"token"` // ex HashedPassword, etc
	Status    types.Status       `db:"status" json:"status"`
	CreatedAt time.Time          `db:"created_at" json:"created_at"`
	UpdatedAt time.Time          `db:"updated_at" json:"updated_at"`
}

type Claims struct {
	UserID   string
	TenantID string
}

func NewAuth(userID string, provider types.AuthProvider, token string) *Auth {
	return &Auth{
		UserID:    userID,
		Provider:  provider,
		Token:     token,
		Status:    types.StatusPublished,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
