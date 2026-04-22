package domain

import "errors"

var (
	ErrNotFound              = errors.New("not found")
	ErrAlreadyExists         = errors.New("version already exists")
	ErrInvalidVersion        = errors.New("invalid version: must be vMAJOR.MINOR.PATCH")
	ErrInvalidInput          = errors.New("invalid input")
	ErrNoChannelHistory      = errors.New("no previous version to roll back to")
	ErrSignedURLUnsupported  = errors.New("signed URLs not supported by this storage backend")
)
