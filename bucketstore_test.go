package aion

import (
	"math"
	"testing"
	"time"
)

func TestBucketStore(t *testing.T) {
	store := NewBucketStore(60*time.Second, math.Pow10(1))
	filter := NewAggregateFilter(0, []string{"raw"}, nil)
	level := Level{
		Filter: filter,
		Store:  store,
	}
	testLevel(&level, t, time.Second, store.Duration)
}
