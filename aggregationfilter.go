package timedb

import (
	"time"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/aggregate"
)

type AggregationFilter struct {
	Granularity time.Duration
	aggregators map[string][]aggregate.Aggregator
	handler (func(uuid.UUID, Entry) error)
}

func (self *AggregationFilter) Insert(series uuid.UUID, entry Entry) error {
	// TODO
	return nil
}

func (self *AggregationFilter) SetHandler(handler (func(uuid.UUID, Entry) error)) {
	self.handler = handler
}
