package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"testing"
)

func testTagStore(store TagStore, t *testing.T) {
	destinationTag := Tag{
		Name:  "destination",
		Value: "google.com",
	}
	series := uuid.NewRandom()
	testTags := []Tag{destinationTag, Tag{"source", "probe0"}}
	series2 := uuid.NewRandom()
	testTags2 := []Tag{destinationTag, Tag{"source", "probe1"}}
	err := store.Tag(series, testTags)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Tag(series2, testTags2)
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.Find([]Tag{destinationTag})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Errorf("Wrong number of series returned (%d != 2)\n", len(res))
	}
}
