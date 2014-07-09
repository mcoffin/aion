package timedb

import (
    "time"
)

type QueryLevel struct {
    Duration time.Duration
    Aggregations []AggregationLevel
    Storer *Storer
}

type AggregationLevel struct {
    Period time.Duration
}
