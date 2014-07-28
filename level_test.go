package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"testing"
	"time"
)

func testLevel(level *Level, t *testing.T, granularity time.Duration, duration time.Duration) {
	series := uuid.NewRandom()
	level.Filter.SetHandler(level.Store.Insert)
	current := time.Now()
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
}
