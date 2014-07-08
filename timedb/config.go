package timedb

import (
    "time"
)

type TimeDB struct {
    Levels []QueryLevel
    Cacher Cacher
}

type QueryLevel struct {
    Duration time.Duration
    Aggregations []AggregationLevel
}

type AggregationLevel struct {
    Period time.Duration
}
