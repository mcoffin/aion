package tags

import "code.google.com/p/go-uuid/uuid"

type Searcher interface {
	Insert(series uuid.UUID, tags []Tag) error
	Find(tags []Tag) (<-chan uuid.UUID, error)
}
