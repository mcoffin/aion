package dynamodb

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion"
	"github.com/crowdmob/goamz/dynamodb"
)

type Repository struct {
	Table *dynamodb.Table
}

// Repository implements the BucketRepository interface
func (self Repository) Get(series uuid.UUID, duration time.Duration, start time.Time, attributes []string) ([]aion.EncodedBucketAttribute, error) {
	hashKey := fmt.Sprintf("%s|%d", series.String(), int64(duration.Seconds()))
	comparisons := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", hashKey),
		*dynamodb.NewEqualInt64AttributeComparison("time", start.Unix()),
	}
	items, err := self.Table.Query(comparisons)
	if err != nil {
		return nil, err
	}
	// Shoule really only loop once
	for _, item := range items {
		ret := make([]aion.EncodedBucketAttribute, len(item))
		i := 0
		if attributes == nil {
			for name, a := range item {
				data, _ := base64.StdEncoding.DecodeString(a.Value)
				ret[i] = aion.EncodedBucketAttribute{
					Name: name,
					Data: data,
				}
			}
		} else {
			for _, name := range attributes {
				a := item[name]
				data, _ := base64.StdEncoding.DecodeString(a.Value)
				ret[i] = aion.EncodedBucketAttribute{
					Name: name,
					Data: data,
				}
			}
		}
		return ret, nil
	}
	// This case is only hit when there was no bucket queried
	return nil, nil
}

// Repository implements the BucketRepository interface
func (self Repository) Put(series uuid.UUID, duration time.Duration, start time.Time, attributes []aion.EncodedBucketAttribute) error {
	hashKey := fmt.Sprintf("%s|%d", series.String(), int64(duration.Seconds()))
	rangeKey := fmt.Sprintf("%d", start.Unix())
	bAttribs := make([]dynamodb.Attribute, len(attributes))
	for i, encodedAttribute := range attributes {
		bAttribs[i] = dynamodb.Attribute{
			Type:  dynamodb.TYPE_BINARY,
			Name:  encodedAttribute.Name,
			Value: base64.StdEncoding.EncodeToString(encodedAttribute.Data),
		}
	}
	_, err := self.Table.PutItem(hashKey, rangeKey, bAttribs)
	return err
}

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
