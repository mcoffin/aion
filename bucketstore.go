package timedb

import (
    "time"
)

type BucketStore struct {
    Duration time.Duration
    Granularities []time.Duration
    Aggregations []string
}

func (self *BucketStore) roundTime(t time.Time) time.Time {
    delta := t.Unix() % int64(self.Duration.Seconds())
    return time.Unix(t.Unix() - delta, 0)
}

func (self *BucketStore) bucketIndices(granularity time.Duration, aggregation string) (int, int) {
    index := 0
    delta := 0
    for i, g := range self.Granularities {
        if g == granularity {
            index = i * (len(self.Aggregations) + 1)
            break
        }
    }
    for i, a := range self.Aggregations {
        if a == aggregation {
            delta = i
            break
        }
    }
    return index, index + delta
}
