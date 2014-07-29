package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/crowdmob/goamz/dynamodb"
	"strconv"
	"time"
)

type DynamoDBStore struct {
	BucketStore
	DynamoDBRepository
}

func NewDynamoDBStore(store BucketStore, table *dynamodb.Table) *DynamoDBStore {
	ret := &DynamoDBStore{
		store,
		DynamoDBRepository{
			Table: table,
		},
	}
	ret.Repository = ret
	return ret
}

type DynamoDBRepository struct {
	Table *dynamodb.Table
}

type DynamoDBCache struct {
	Table *dynamodb.Table
}

func (self *DynamoDBCache) Query(series uuid.UUID, start, end time.Time, attributes []string) (EntryReader, error) {
	conditions := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", series.String()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_GREATER_THAN_OR_EQUAL, start.Unix()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_LESS_THAN, end.Unix()),
	}
	items, err := self.Table.Query(conditions)
	if err != nil {
		return nil, err
	}
	index := 0
	ret := func(entries []Entry) (int, error) {
		end := index + len(entries)
		if end > len(items) {
			end = len(items)
		}
		itemCount := 0
		for i, item := range items[index:end] {
			unixTime, err := strconv.ParseInt(item["time"].Value, 10, 64)
			if err != nil {
				return i, err
			}
			e := Entry{
				Timestamp:  time.Unix(unixTime, 0),
				Attributes: map[string]float64{},
			}
			for name, attrib := range item {
				if name == "time" {
					continue
				}
				floatValue, err := strconv.ParseFloat(attrib.Value, 64)
				if err != nil {
					return i, err
				}
				e.Attributes[name] = floatValue
			}
			entries[i] = e
			itemCount = i + 1
		}
		return itemCount, nil
	}
	return queryFunc(ret), nil
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
