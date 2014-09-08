package dynamodb

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/tags"
	"github.com/crowdmob/goamz/dynamodb"
)

type TagStore struct {
	Table dynamodb.Table
}

func (self *TagStore) Insert(series uuid.UUID, ts []tags.Tag) error {
	_, err := self.Table.PutItem(series.String(), "", convertTagsToAttributes(ts))
	return err
}

func (self *TagStore) Query(series uuid.UUID) ([]tags.Tag, error) {
	attributes, err := self.Table.GetItem(&dynamodb.Key{HashKey: series.String()})
	if err != nil {
		return nil, err
	}
	ts := make([]tags.Tag, 0, len(attributes))
	for _, a := range attributes {
		ts = append(ts, tags.Tag{
			Name:  a.Name,
			Value: a.Value,
		})
	}
	return ts, nil
}

func (self *TagStore) Scan() (<-chan tags.Series, error) {
	result, err := self.Table.Scan([]dynamodb.AttributeComparison{})
	if err != nil {
		return nil, err
	}
	out := make(chan tags.Series)
	go func() {
		defer close(out)
		for _, m := range result {
			s := tags.Series{
				Tags: make([]tags.Tag, 0, len(m)-1),
			}
			for _, a := range m {
				if a.Name == "series" {
					s.SeriesID = uuid.Parse(a.Value)
				} else {
					s.Tags = append(s.Tags, tags.Tag{
						Name:  a.Name,
						Value: a.Value,
					})
				}
			}
			out <- s
		}
	}()
	return out, nil
}

func convertTagsToAttributes(ts []tags.Tag) []dynamodb.Attribute {
	attribs := make([]dynamodb.Attribute, 0, len(ts))
	for _, t := range ts {
		attribs = append(attribs, dynamodb.Attribute{
			Type:  dynamodb.TYPE_STRING,
			Name:  t.Name,
			Value: t.Value,
		})
	}
	return attribs
}
