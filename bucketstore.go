package timedb

import (
    "time"
)

type BucketStore struct {
    Duration time.Duration
    Granularities []time.Duration
    Aggregations []string
}

func (self *BucketStore) nearestStart(t time.Time) time.Time {
    delta := t.Unix() % int64(self.Duration.Seconds())
    return time.Unix(t.Unix() - delta, 0)
}
