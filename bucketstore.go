package timedb

import (
    "time"
)

// A representation of a backing store that keeps data in blocks and buckets
type BucketStore struct {
    Duration time.Duration
    Granularities []time.Duration
    Aggregations []string
}

// Rounds a time based on the duration of this BucketStore
func (self *BucketStore) nearestStart(t time.Time) time.Time {
    delta := t.Unix() % int64(self.Duration.Seconds())
    return time.Unix(t.Unix() - delta, 0)
}
