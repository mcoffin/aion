package tagstore

import "code.google.com/p/go-uuid/uuid"

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type TagStore interface {
	Tag(series uuid.UUID, tags []Tag) error
	Find(tags []Tag) (<-chan uuid.UUID, error)
}
