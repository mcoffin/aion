package aion

import (
	"github.com/google/cayley/graph"
	_ "github.com/google/cayley/graph/memstore"
	"testing"
)

func TestCayleyTagStore(t *testing.T) {
	ts, err := graph.NewTripleStore("memstore", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	store := CayleyTagStore{
		TripleStore: ts,
	}
	testTagStore(store, t)
}
