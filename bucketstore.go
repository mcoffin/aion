package timedb

type BucketStore struct {
    Duration time.Duration
    Granularity time.Duration
    Aggregations []string
    Multiplier float64
    Storer BucketStorer
}

func (self *BucketStore) Insert(entries chan Entry, errors chan error) {
    for {
        entry, more := <-entries
        if !more {
            return
        }
        start := entry.Timestamp.Truncate(self.Duration)
    }
}
