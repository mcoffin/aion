package cayley

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/google/cayley/graph"
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

func (self TagStore) Tag(series uuid.UUID, tags []aion.Tag) error {
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

func (self TagStore) Find(tags []aion.Tag) ([]uuid.UUID, error) {
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
	ret := make([]uuid.UUID, count)
	count = 0
	for _, item := range genRes {
		m := item.(map[string]string)
		ret[count] = uuid.Parse(m["id"])
		count++
	}
	return ret[:count], nil
}
