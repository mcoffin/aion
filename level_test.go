package aion_test

import (
	"testing"
	"time"

	"github.com/FlukeNetworks/aion"

	"code.google.com/p/go-uuid/uuid"
)

func testLevel(level *aion.Level, t *testing.T, granularity time.Duration, duration time.Duration) {
	series := uuid.NewRandom()
	level.Filter.SetHandler(level.Store.Insert)
	start := time.Now()
	current := start
	end := current.Add(duration)
	insertCount := 0
	for !current.After(end) {
		for _, v := range testData {
			e := aion.Entry{
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
	entryC := make(chan aion.Entry)
	errorC := make(chan error)
	go func() {
		defer close(entryC)
		level.Store.Query(series, start, end, []string{"raw"}, entryC, errorC)
	}()
	queryCount := 0

	sStart := start.Truncate(time.Second)
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
			if e.Attributes["raw"] != testData[testDataIndex] {
				t.Errorf("Value %v at index %d doesn't match %v\n", e.Attributes["raw"], i, testData[testDataIndex])
			}
			if !e.Timestamp.Truncate(time.Second).Equal(sStart) {
				t.Errorf("Time %d at index %d doesn't match %d\n", e.Timestamp.Unix(), i, sStart.Unix())
			}
			sStart = sStart.Add(granularity)
			queryCount++
		}
	}
	if queryCount != insertCount {
		t.Errorf("Inserted item count %d doesn't match queried item count %d\n", insertCount, queryCount)
	}
}
