package aion

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/base64"
	"fmt"
	"github.com/FlukeNetworks/aion/bucket"
	"github.com/crowdmob/goamz/dynamodb"
	"strconv"
	"time"
)

// A DynamoDBStore is a bucket storage implementation using DynamoDB as its backing
type DynamoDBStore struct {
	BucketStore
	repo DynamoDBRepository
}

// Creates a new DynamoDBStore
func NewDynamoDBStore(store BucketStore, table *dynamodb.Table, multiplier float64, duration time.Duration) *DynamoDBStore {
	ret := &DynamoDBStore{
		store,
		DynamoDBRepository{
			Multiplier:  multiplier,
			Granularity: store.Granularity,
			Duration:    duration,
			Table:       table,
		},
	}
	ret.Repository = ret.repo
	return ret
}

// Implementation of the BucketRepository interface using DynamoDB to store buckets
type DynamoDBRepository struct {
	Multiplier  float64
	Granularity time.Duration
	Duration    time.Duration
	Table       *dynamodb.Table
}

// DynamoDBRepository implements the BucketRepository interface
func (self DynamoDBRepository) Put(series uuid.UUID, granularity time.Duration, start time.Time, attributes []EncodedBucketAttribute) error {
	hashKey := fmt.Sprintf("%s|%d", series.String(), int64(granularity.Seconds()))
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

// Convenience function for creating an EntryReader from dynamodb query results
func (self DynamoDBRepository) entryReader(series uuid.UUID, item map[string]*dynamodb.Attribute, attributes []string) (EntryReader, error) {
	tData, err := base64.StdEncoding.DecodeString(item[TimeAttribute].Value)
	if err != nil {
		return nil, err
	}
	startUnix, err := strconv.ParseInt(item["time"].Value, 10, 64)
	if err != nil {
		return nil, err
	}
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(startUnix, bytes.NewBuffer(tData)),
	}
	for _, a := range attributes {
		data, err := base64.StdEncoding.DecodeString(item[a].Value)
		if err != nil {
			return nil, err
		}
		decs[a] = bucket.NewBucketDecoder(0, bytes.NewBuffer(data))
	}
	return bucketEntryReader(series, self.Multiplier, decs, attributes), nil
}

// DynamoDBRepository implements the BucketRepository interface
func (self DynamoDBRepository) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	timeComparison := dynamodb.AttributeComparison{
		AttributeName:      "time",
		ComparisonOperator: dynamodb.COMPARISON_BETWEEN,
		AttributeValueList: []dynamodb.Attribute{
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_NUMBER,
				Name:  "time",
				Value: fmt.Sprintf("%d", start.Truncate(self.Duration).Unix()),
			},
			dynamodb.Attribute{
				Type:  dynamodb.TYPE_NUMBER,
				Name:  "time",
				Value: fmt.Sprintf("%d", end.Unix()),
			},
		},
	}
	comparisons := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", fmt.Sprintf("%s|%d", series.String(), int64(self.Granularity.Seconds()))),
		timeComparison,
	}
	items, err := self.Table.Query(comparisons)
	if err != nil {
		errors <- err
		return
	}
	for _, item := range items {
		reader, err := self.entryReader(series, item, attributes)
		if err != nil {
			errors <- err
			return
		}
		entryBuf := make([]Entry, 1)
		entryBackBuf := make([]Entry, len(entryBuf))
		for i, _ := range entryBuf {
			entryBuf[i].Attributes = map[string]float64{}
			entryBackBuf[i].Attributes = map[string]float64{}
		}
		for {
			n, err := reader.ReadEntries(entryBuf)
			tmp := entryBuf
			entryBuf = entryBackBuf
			entryBackBuf = tmp
			if n > 0 {
				for _, e := range entryBackBuf[:n] {
					entries <- e
				}
			}
			if err != nil {
				break
			}
		}
	}
}

// Implementation of a seriesStore using DynamoDB to cache raw data
type DynamoDBCache struct {
	Table *dynamodb.Table
}

// DynamoDBCache implements the SeriesStore interface
func (self *DynamoDBCache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
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
		e := Entry{
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

// DynamoDBCache implements the SeriesStore interface
func (self *DynamoDBCache) Insert(series uuid.UUID, entry Entry) error {
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
