package aion

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/aggregate"
	"time"
)

// A FilterBase contains the components that every filter will contain
type FilterBase struct {
	handler (func(uuid.UUID, Entry) error)
}

// FilterBase implements part of the Filter interface
func (self *FilterBase) SetHandler(handler func(uuid.UUID, Entry) error) {
	self.handler = handler
}

// context for a bunch of aggregations over a period
type aggregatorContext struct {
	end         time.Time
	aggregators map[string]aggregate.Aggregator
}

// an AggregateFilter is a filter that uses the `aggregate` package to roll up data
type AggregateFilter struct {
	FilterBase
	Granularity time.Duration
	Aggregators []string
	contexts    map[string]map[time.Time]*aggregatorContext
}

// Creates a new AggregateFilter
func NewAggregateFilter(granularity time.Duration, aggregators []string, handler func(uuid.UUID, Entry) error) *AggregateFilter {
	return &AggregateFilter{
		FilterBase: FilterBase{
			handler: handler,
		},
		Granularity: granularity,
		Aggregators: aggregators,
		contexts:    map[string]map[time.Time]*aggregatorContext{},
	}
}

// Convenience method for getting the start time of the aggregatorContext to which `t`
// should belong
func (self AggregateFilter) aggregatorTime(t time.Time) time.Time {
	return t.Truncate(self.Granularity)
}

// Convenience method for getting a context if it exists, and creating/filling it if it doesn't
func (self *AggregateFilter) context(series uuid.UUID, entry Entry) (*aggregatorContext, error) {
	seriesStr := series.String()
	if self.contexts[seriesStr] == nil {
		self.contexts[seriesStr] = map[time.Time]*aggregatorContext{}
	}
	t := self.aggregatorTime(entry.Timestamp)
	if self.contexts[seriesStr][t] == nil {
		aggs := make(map[string]aggregate.Aggregator, len(self.Aggregators))
		for _, name := range self.Aggregators {
			a, err := aggregate.NewAggregator(name)
			if err != nil {
				return nil, err
			}
			aggs[name] = a
		}
		self.contexts[seriesStr][t] = &aggregatorContext{
			end:         t.Add(self.Granularity),
			aggregators: aggs,
		}
	}
	return self.contexts[seriesStr][t], nil
}

// AggregateFilter implements the Filter interface
func (self *AggregateFilter) Insert(series uuid.UUID, entry Entry) error {
	// First, add the new value to the context it needs to be in
	ctx, err := self.context(series, entry)
	if err != nil {
		return err
	}
	for name, a := range ctx.aggregators {
		a.Add(entry.Attributes[name])
	}
	// Dump all completed contexts for this series to handler
	seriesContexts := self.contexts[series.String()]
	for t, c := range seriesContexts {
		if entry.Timestamp.After(c.end) || self.Granularity == 0 {
			e := Entry{
				Timestamp:  t,
				Attributes: map[string]float64{},
			}
			for name, a := range ctx.aggregators {
				e.Attributes[name] = a.Value()
			}
			delete(seriesContexts, t)
			err = self.handler(series, e)
		}
	}
	return err
}
