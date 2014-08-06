package aion

import (
	"testing"
)

func TestCacheFilter(t *testing.T) {
	filter := AggregationFilter{
		Granularity:  0,
		Aggregations: []string{"raw"},
	}
	filter.Init()
	testFilter(&filter, t)
}
