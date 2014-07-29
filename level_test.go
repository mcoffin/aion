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
	buf := make([]Entry, 8)
	reader, err := level.Store.Query(series, start, end, []string{"raw"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		n, err := reader.ReadEntries(buf)
		if n > 0 {
			for _, e := range buf[:n] {
				fmt.Println("query: %+v\n", e)
			}
		}
		if err != nil {
			break
		}
	}
}
