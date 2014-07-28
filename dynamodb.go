package timedb

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/crowdmob/goamz/dynamodb"
)

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
