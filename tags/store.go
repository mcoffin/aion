package tags

import "code.google.com/p/go-uuid/uuid"

type Series struct {
	SeriesID uuid.UUID
	Tags     []Tag
}

type Store interface {
	Insert(series uuid.UUID, tags []Tag) error
	Query(series uuid.UUID) ([]Tag, error)
	Scan() (<-chan Series, error)
}
