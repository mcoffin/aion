package meta

import "code.google.com/p/go-uuid/uuid"

type Store interface {
	Index(id uuid.UUID, metadata interface{}) error
	GetMetadata(id uuid.UUID, data interface{}) error
}

type Searcher interface {
	Search(query string) (<-chan uuid.UUID, error)
}
