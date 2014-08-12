package aion

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/query/gremlin"
	"time"
)

type CayleyTagStore struct {
	TripleStore graph.TripleStore
}

func (self CayleyTagStore) createSession() *gremlin.Session {
	// TODO: come up with a sensible timeout
	return gremlin.NewSession(self.TripleStore, 60*time.Second, true)
}

func (self CayleyTagStore) Tag(series uuid.UUID, tags []Tag) error {
	triples := make([]quad.Quad, len(tags))
	for i, t := range tags {
		triples[i] = quad.Quad{
			Subject:   series.String(),
			Predicate: t.Name,
			Object:    t.Value,
		}
	}
	self.TripleStore.AddTripleSet(triples)
	return nil
}

func (self CayleyTagStore) Find(tags []Tag) ([]uuid.UUID, error) {
	session := self.createSession()
	tagsJson, err := json.Marshal(tags)
	if err != nil {
		return nil, err
	}
	queryBuilder := bytes.NewBufferString("var tags = JSON.parse('")
	queryBase := `
	var queries = [];
	tags.forEach(function(tag, i) {
    	queries[i] = g.V().Has(tag.name, tag.value)
	});
	var result = queries[0];
	var nQueries = queries.length;
	for (var i = 1; i < nQueries; i++) {
    	result = result.Intersect(queries[i]);
	}
	result.All();
	`
	fmt.Fprintf(queryBuilder, "%s');\n%s", string(tagsJson), queryBase)
	series := make(chan map[string]interface{})
	go session.GetQuery(queryBuilder.String(), series)
	for item := range series {
		fmt.Println(item)
	}
	return nil, nil
}
