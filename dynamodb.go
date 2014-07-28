package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/base64"
	"fmt"
	"github.com/crowdmob/goamz/dynamodb"
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
