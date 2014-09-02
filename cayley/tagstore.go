package cayley

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/tagstore"
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/leveldb"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/query/gremlin"
)

type TagStore struct {
	TripleStore graph.TripleStore
}

func (self TagStore) createSession() *gremlin.Session {
	// TODO: come up with a sensible timeout
	return gremlin.NewSession(self.TripleStore, 60*time.Second, true)
}

func (self TagStore) Tag(series uuid.UUID, tags []tagstore.Tag) error {
	triples := make([]quad.Quad, len(tags))
	for i, t := range tags {
		triples[i] = quad.Quad{
			Subject:   series.String(),
			Predicate: t.Name,
			Object:    t.Value,
		}
	}
	self.TripleStore.AddTripleSet(triples) // TODO: Error handling? lol
	return nil
}

func (self TagStore) Find(tags []tagstore.Tag) (<-chan uuid.UUID, error) {
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
	series := make(chan interface{})
	go session.ExecInput(queryBuilder.String(), series, 0)
	count := 0
	for item := range series {
		count++
		session.BuildJson(item)
	}
	genRes, err := session.GetJson()
	if err != nil {
		return nil, err
	}
	out := make(chan uuid.UUID)
	go func() {
		defer close(out)
		for _, item := range genRes {
			m := item.(map[string]string)
			out <- uuid.Parse(m["id"])
		}
	}()
	return out, nil
}
