package timedb

import (
	"testing"
	"time"
	"code.google.com/p/go-uuid/uuid"
)

func testLevel(level *Level, t *testing.T) {
	series := uuid.NewRandom()
	level.Filter.SetHandler(level.Store.Insert)
	current := time.Now()
	for _, v := range testData {
		e := Entry{
			Timestamp: current,
			Attributes: map[string]float64{"raw": v},
		}
		err := level.Filter.Insert(series, e)
		if err != nil {
			t.Error(err)
		}
		current = current.Add(testSpan)
	}
}
