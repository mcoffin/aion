package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"testing"
	"time"
)

func testLevel(level *Level, t *testing.T, granularity time.Duration, duration time.Duration) {
	series := uuid.NewRandom()
	level.Filter.SetHandler(level.Store.Insert)
	start := time.Now()
	current := start
	end := current.Add(duration)
	insertCount := 0
	for !current.After(end) {
		for _, v := range testData {
			e := Entry{
				Timestamp:  current,
				Attributes: map[string]float64{"raw": v},
			}
			err := level.Filter.Insert(series, e)
			insertCount++
			if err != nil {
				t.Error(err)
			}
			current = current.Add(granularity)
			if current.After(end) {
				break
			}
		}
	}
	level.Filter.Flush(series)
	entryC := make(chan Entry)
	errorC := make(chan error)
	go func() {
		defer close(entryC)
		level.Store.Query(series, start, end, []string{"raw"}, entryC, errorC)
	}()
	queryCount := 0
loop:
	for i := 0; true; i++ {
		select {
		case err := <-errorC:
			t.Error(err)
			i--
		case e, more := <-entryC:
			if !more {
				break loop
			}
			testDataIndex := i % len(testData)
			fmt.Printf("Index %d\n", i)
			if e.Attributes["raw"] != testData[testDataIndex] {
				t.Errorf("Value %v at index %d doesn't match %v\n", e.Attributes["raw"], i, testData[testDataIndex])
			}
			queryCount++
		}
	}
	if queryCount != insertCount {
		t.Errorf("Inserted item count %d doesn't match queried item count %d\n", insertCount, queryCount)
	}
}
