package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"testing"
)

func testTagStore(store TagStore, t *testing.T) {
	series := uuid.NewRandom()
	testTags := []Tag{Tag{"destination", "google.com"}, Tag{"source", "probe0"}}
	err := store.Tag(series, testTags)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Find(testTags)
}
