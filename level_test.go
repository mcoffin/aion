package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"testing"
	"time"
)

func testLevel(level *Level, t *testing.T, granularity time.Duration, duration time.Duration) {
	series := uuid.NewRandom()
	level.Filter.SetHandler(level.Store.Insert)
	start := time.Now()
	current := start
	end := current.Add(duration)
	for !current.After(end) {
		for _, v := range testData {
			e := Entry{
				Timestamp:  current,
				Attributes: map[string]float64{"raw": v},
			}
			err := level.Filter.Insert(series, e)
			if err != nil {
				t.Error(err)
			}
			current = current.Add(granularity)
		}
	}
	entryC := make(chan Entry, 4)
	errorC := make(chan error)
	go level.Store.Query(series, start, end, []string{"raw"}, entryC, errorC)
	for i := 0; true; i++ {
		select {
		case err := <-errorC:
			t.Error(err)
			return
		case e, more := <-entryC:
			if !more {
				return
			}
			testDataIndex := i % len(testData)
			if e.Attributes["raw"] != testData[testDataIndex] {
				t.Errorf("Value %v at index %d doesn't match %v\n", e.Attributes["raw"], i, testData[testDataIndex])
			}
		}
	}
}
