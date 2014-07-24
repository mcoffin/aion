package timedb

import (
	"testing"
	"time"
	"code.google.com/p/go-uuid/uuid"
)

var testData = []float64{79.1, 80.0, 78.2, 43.1, 90.7, 90.7}

const (
	testSpan = 1 * time.Second
)

func testFilter(f Filter, t *testing.T) {
	seriesUUID := uuid.NewRandom()
	checkIndex := 0
	f.SetHandler(func(series uuid.UUID, entry Entry) error {
		val := entry.Attributes["raw"]
		if val != testData[checkIndex] {
			t.Errorf("%v at index %d != %v\n", val, checkIndex, testData[checkIndex])
		}
		checkIndex++
		return nil
	})
	current := time.Now()
	for _, v := range testData {
		e := Entry{
			Timestamp: current,
			Attributes: map[string]float64{"raw": v},
		}
		f.Insert(seriesUUID, e)
		current = current.Add(testSpan)
	}
}
