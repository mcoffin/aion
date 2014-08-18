package cayley_test

import (
	"testing"

	"github.com/FlukeNetworks/aion/aiontest"
	"github.com/FlukeNetworks/aion/cayley"
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
)

func TestTagStore(t *testing.T) {
	ts, err := graph.NewTripleStore("memstore", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	store := cayley.TagStore{
		TripleStore: ts,
	}
	aiontest.TestTagStore(store, t)
}
