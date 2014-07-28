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

func (self *DynamoDBRepository) Put(series uuid.UUID, context *SeriesBucketStoreContext, store *BucketStore) error {
	attribs := make([]dynamodb.Attribute, len(context.Contexts)+1)
	attribs[0] = dynamodb.Attribute{
		Type:  "N",
		Name:  "baseline",
		Value: fmt.Sprintf("%d", context.Baseline),
	}
	i := 1
	for name, ctx := range context.Contexts {
		attribs[i] = dynamodb.Attribute{
			Type:  "B",
			Name:  name,
			Value: base64.StdEncoding.EncodeToString(ctx.Buffer.Bytes()),
		}
		i++
	}
	// I really wish dynamodb had multiple-attribute keys because this manual encoding of they key sucks
	_, err := self.Table.PutItem(fmt.Sprintf("%s|%d", series.String(), int64(store.Duration.Seconds())), fmt.Sprintf("%v", context.Start(store).Unix()), attribs)
	return err
}

type DynamoDBCache struct {
	Table *dynamodb.Table
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

func (self *DynamoDBCache) Query(series uuid.UUID, start time.Time, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	conditions := []dynamodb.AttributeComparison{
		*dynamodb.NewEqualStringAttributeComparison("series", series.String()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_GREATER_THAN_OR_EQUAL, start.Unix()),
		*dynamodb.NewNumericAttributeComparison("time", dynamodb.COMPARISON_LESS_THAN, end.Unix()),
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
					return
				}
				e.Timestamp = time.Unix(unixTime, 0)
			} else {
				value, err := strconv.ParseFloat(a.Value, 64)
				if err != nil {
					errors <- err
					return
				}
				e.Attributes[name] = value
			}
		}
		entries <- e
	}
	close(entries)
}
