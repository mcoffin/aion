package elastisearch

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/mattbaird/elastigo/lib"
)

const (
	documentType = "series"
)

type Metastore struct {
	Connection *elastigo.Conn
	IndexName  string
}

// Metastore implements the meta.Store interface
func (self *Metastore) Index(id uuid.UUID, metadata interface{}) error {
	_, err := self.Connection.Index(self.IndexName, documentType, id.String(), nil, metadata)
	return err
}

// Metastore implements the meta.Store interface
func (self *Metastore) GetMetadata(id uuid.UUID, data interface{}) error {
	_, err := self.Connection.Get(self.IndexName, documentType, id.String(), nil)
	return err
}

// Metastore implements the meta.Searcher interface
func (self *Metastore) Search(query string) (<-chan uuid.UUID, error) {
	res, err := self.Connection.Search(self.IndexName, documentType, nil, query)
	if err != nil {
		return nil, err
	}

	out := make(chan uuid.UUID)
	go func() {
		defer close(out)
		for _, h := range res.Hits.Hits {
			out <- uuid.Parse(h.Id)
		}
	}()

	return out, nil
}
