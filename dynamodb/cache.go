package dynamodb

import (
	"fmt"
	"strconv"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/crowdmob/goamz/dynamodb"
)

// Implementation of a seriesStore using DynamoDB to cache raw data
type Cache struct {
	Table *dynamodb.Table
}

// Cache implements the SeriesStore interface
func (self Cache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan aion.Entry, errors chan error) {
	timeComparison := dynamodb.AttributeComparison{
		AttributeName:      "time",
		ComparisonOperator: dynamodb.COMPARISON_BETWEEN,
		AttributeValueList: []dynamodb.Attribute{
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_NUMBER,
				Name:  "time",
				Value: fmt.Sprintf("%d", start.Unix()),
			},
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_NUMBER,
				Name:  "time",
				Value: fmt.Sprintf("%d", end.Unix()),
			},
		},
	}
	conditions := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", series.String()),
		timeComparison,
	}
	items, err := self.Table.Query(conditions)
	if err != nil {
		errors <- err
		return
	}
	for _, item := range items {
		e := aion.Entry{
			Attributes: map[string]float64{},
		}
		for name, a := range item {
			if name == "series" {
				continue
			}
			if name == "time" {
				unixTime, err := strconv.ParseInt(a.Value, 10, 64)
				if err != nil {
					errors <- err
					continue
				}
				e.Timestamp = time.Unix(unixTime, 0)
			} else {
				value, err := strconv.ParseFloat(a.Value, 64)
				if err != nil {
					errors <- err
					continue
				}
				e.Attributes[name] = value
			}
		}
		entries <- e
	}
}

// Cache implements the SeriesStore interface
func (self Cache) Insert(series uuid.UUID, entry aion.Entry) error {
	attribs := []dynamodb.Attribute{
		dynamodb.Attribute{
			Type:  "N",
			Name:  "raw",
			Value: fmt.Sprintf("%v", entry.Attributes["raw"]),
		},
	}
	_, err := self.Table.PutItem(series.String(), fmt.Sprintf("%v", entry.Timestamp.Unix()), attribs)
	return err
}
