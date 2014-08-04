package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/timedb/aggregate"
	"time"
)

type aggregationContext struct {
	start, end time.Time
}

func (self *aggregationContext) reset(t time.Time, granularity time.Duration) {
	self.start = t.Truncate(granularity)
	self.end = self.start.Add(granularity)
}

type AggregationFilter struct {
	Granularity  time.Duration
	Aggregations []string
	aggregators  map[string]map[string]aggregate.Aggregator
	aContexts    map[string]*aggregationContext
	handler      (func(uuid.UUID, Entry) error)
}

func (self *AggregationFilter) Init() {
	self.aggregators = map[string]map[string]aggregate.Aggregator{}
	self.aContexts = map[string]*aggregationContext{}
}

func (self *AggregationFilter) Flush(series uuid.UUID) error {
	seriesStr := series.String()
	aggregators := self.aggregators[seriesStr]
	e := Entry{
		Timestamp:  self.aContexts[seriesStr].start,
		Attributes: map[string]float64{},
	}
	for name, a := range aggregators {
		e.Attributes[name] = a.Value()
		a.Reset()
	}
	self.aContexts[seriesStr] = nil
	return self.handler(series, e)
}

func (self *AggregationFilter) Insert(series uuid.UUID, entry Entry) error {
	var err error
	seriesStr := series.String()
	// First-time setup
	if self.aContexts[seriesStr] == nil {
		ctx := &aggregationContext{}
		ctx.reset(entry.Timestamp, self.Granularity)
		self.aContexts[seriesStr] = ctx

		aggs := map[string]aggregate.Aggregator{}
		for _, name := range self.Aggregations {
			a, err := aggregate.NewAggregator(name)
			if err != nil {
				return err
			}
			aggs[name] = a
		}
		self.aggregators[seriesStr] = aggs
	}
	aggregators := self.aggregators[seriesStr]
	if entry.Timestamp.After(self.aContexts[seriesStr].end) {
		e := Entry{
			Timestamp:  self.aContexts[seriesStr].start,
			Attributes: map[string]float64{},
		}
		for name, a := range aggregators {
			e.Attributes[name] = a.Value()
			a.Reset()
		}
		self.aContexts[seriesStr].reset(entry.Timestamp, self.Granularity)
		err = self.handler(series, e)
	}

	// Add new value to all aggregators
	for name, value := range entry.Attributes {
		aggregators[name].Add(value)
	}

	if self.Granularity == 0 {
		self.Flush(series)
	}

	return err
}

func (self *AggregationFilter) SetHandler(handler func(uuid.UUID, Entry) error) {
	self.handler = handler
}
