package timedb

import (
	"time"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/aggregate"
)

type aggregationContext struct {
	start, end time.Time
}

func (self *aggregationContext) reset(t time.Time, granularity time.Duration) {
	self.start = t.Truncate(granularity)
	self.end = self.start.Add(granularity)
}

type AggregationFilter struct {
	Granularity time.Duration
	aggregators map[string][]aggregate.Aggregator
	aContexts map[string]*aggregationContext
	handler (func(uuid.UUID, Entry) error)
}

func (self *AggregationFilter) Insert(series uuid.UUID, entry Entry) error {
	seriesStr := series.String()
	if self.aContexts[seriesStr] == nil {
		ctx := new(aggregationContext)
		ctx.reset(entry.Timestamp, self.Granularity)
		self.aContexts[seriesStr] = ctx
		// TODO: create aggregators
	}
	aggregators := self.aggregators[series.String()]
	for _, a := range aggregators {
		a.Add(entry.Value)
	}
}

func (self *AggregationFilter) SetHandler(handler (func(uuid.UUID, Entry) error)) {
	self.handler = handler
}
