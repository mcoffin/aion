package timedb

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/bucket"
	"time"
)

type bucketStoreContext struct {
	buffer, lastBuffer *bytes.Buffer
	encoder            *bucket.BucketEncoder
}

type BucketStore struct {
	Duration   time.Duration
	Multiplier float64
	contexts   map[string]map[string]*bucketStoreContext
}

func (self *BucketStore) Insert(series uuid.UUID, entry Entry) error {
	contexts := self.contexts[series.String()]
	// TODO: rollup if necessary
	// Write all attributes to their encoders
	for k, v := range entry.Attributes {
		ctx := contexts[k]
		if ctx == nil {
			ctx = &bucketStoreContext{
				buffer: &bytes.Buffer{},
			}
			ctx.encoder = bucket.NewBucketEncoder(marshalFloat64(entry.Attributes[k], self.Multiplier), ctx.buffer)
			contexts[k] = ctx
		}
		contexts[k].encoder.WriteInt(marshalFloat64(v, self.Multiplier))
	}
	return nil
}

func marshalFloat64(v float64, multiplier float64) int64 {
	return int64(v * multiplier)
}
