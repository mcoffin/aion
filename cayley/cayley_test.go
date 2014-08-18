package cayley_test

import (
	"testing"

	"github.com/FlukeNetworks/aion/cayley"
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
)

func TestCayleyTagStore(t *testing.T) {
	ts, err := graph.NewTripleStore("memstore", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	store := cayley.CayleyTagStore{
		TripleStore: ts,
	}
	testTagStore(store, t)
}
