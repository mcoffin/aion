package aiontest

import (
	"testing"

	"github.com/FlukeNetworks/aion"

	"code.google.com/p/go-uuid/uuid"
)

func TestTagStore(store aion.TagStore, t *testing.T) {
	destinationTag := aion.Tag{
		Name:  "destination",
		Value: "google.com",
	}
	series := uuid.NewRandom()
	testTags := []aion.Tag{destinationTag, aion.Tag{"source", "probe0"}}
	series2 := uuid.NewRandom()
	testTags2 := []aion.Tag{destinationTag, aion.Tag{"source", "probe1"}}
	err := store.Tag(series, testTags)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Tag(series2, testTags2)
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.Find([]aion.Tag{destinationTag})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Errorf("Wrong number of series returned (%d != 2)\n", len(res))
	}
}
