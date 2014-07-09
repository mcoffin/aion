package timedb

import (
    "time"
)

type TimeDB struct {
    Levels []QueryLevel
}

type QueryLevel struct {
    Duration time.Duration
    Aggregations []AggregationLevel
}

type AggregationLevel struct {
    Period time.Duration
}
