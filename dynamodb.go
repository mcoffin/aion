package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/base64"
	"fmt"
	"github.com/crowdmob/goamz/dynamodb"
	"strconv"
	"time"
)

type DynamoDBStore struct {
	BucketStore
	repo DynamoDBRepository
}

func NewDynamoDBStore(store BucketStore, table *dynamodb.Table) *DynamoDBStore {
	ret := &DynamoDBStore{
		store,
		DynamoDBRepository{
			Table: table,
		},
	}
	ret.Repository = ret.repo
	return ret
}

type DynamoDBRepository struct {
	Table *dynamodb.Table
}

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

func (self DynamoDBRepository) Get(series uuid.UUID, start time.Time) ([]EncodedBucketAttribute, error) {
	// TODO
	return nil, nil
}

type DynamoDBCache struct {
	Table *dynamodb.Table
}

func (self *DynamoDBCache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	conditions := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", series.String()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_GREATER_THAN_OR_EQUAL, start.Unix()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_LESS_THAN_OR_EQUAL, end.Unix()),
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
