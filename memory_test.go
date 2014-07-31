package timedb

import (
	"math"
	"testing"
	"time"
)

func TestMemoryCache(t *testing.T) {
	store := MemoryBucketBuilder{
		Duration:   60 * time.Second,
		Multiplier: math.Pow10(1),
	}
	store.Init()
	filter := AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	level := Level{
		Filter: &filter,
		Store:  &store,
	}
	testLevel(&level, t, time.Second, store.Duration)
}
