package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"io"
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
	buf := make([]Entry, 9)
	reader, err := level.Store.Query(series, start, end, []string{"raw"})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		n, err := reader.ReadEntries(buf)
		if n > 0 {
			for _, e := range buf[:n] {
				index := count % len(testData)
				if e.Attributes["raw"] != testData[index] {
					t.Errorf("Attribute %v at index %d does not match %v\n", e.Attributes["raw"], count, testData[index])
				}
				count++
			}
		}
		if err != nil {
			if err.Error() != io.EOF.Error() {
				t.Fatal(err)
			} else {
				break
			}
		}
	}
	if insertCount != count {
		t.Errorf("Insert count %d doesn't match query count %d\n", insertCount, count)
	}
}
