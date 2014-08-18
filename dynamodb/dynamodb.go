package dynamodb

import (
	"encoding/base64"
	"fmt"
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
