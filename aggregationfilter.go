package timedb

import (
	"time"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/aggregate"
)

type AggregationFilter struct {
	Granularity time.Duration
	aggregators map[string][]aggregate.Aggregator
	handler func(uuid.UUID, Entry)
}

func (self *AggregationFilter) Insert(series uuid.UUID, entry Entry) {
}

func (self *AggregationFilter) SetHandler(handler func(uuid.UUID, Entry)) {
	self.handler = handler
}
