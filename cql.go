package aion

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"github.com/FlukeNetworks/aion/bucket"
	"github.com/gocql/gocql"
	"time"
)

type CQLStore struct {
	BucketStore
	repo CQLRepository
}

func NewCQLStore(store BucketStore, session *gocql.Session, multiplier float64) *CQLStore {
	ret := &CQLStore{
		store,
		CQLRepository{
			Multiplier: multiplier,
			Granularity: store.Granularity,
			Session: session,
		},
	}
	ret.Repository = ret.repo
	return ret
}

type CQLRepository struct {
	Multiplier float64
	Granularity time.Duration
	Session *gocql.Session
}

// CQLRepository implements the BucketRepository interface
func (self CQLRepository) Put(series uuid.UUID, granularity time.Duration, start time.Time, attributes []EncodedBucketAttribute) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	attribMap := map[string][]byte{}
	for _, encodedAttribute := range attributes {
		attribMap[encodedAttribute.Name] = encodedAttribute.Data
	}
	return self.Session.Query("INSERT INTO buckets (series, time, attribs) VALUES (?, ?, ?)", seriesUUID, start, attribMap).Exec()
}

func (self CQLRepository) entryReader(series uuid.UUID, start time.Time, attribMap map[string][]byte, attributes []string) (EntryReader, error) {
	tData := attribMap[TimeAttribute]
	startUnix := start.Unix()
	decs := map[string]*bucket.BucketDecoder{
		TimeAttribute: bucket.NewBucketDecoder(startUnix, bytes.NewBuffer(tData)),
	}
	for _, a := range attributes {
		data := attribMap[a]
		decs[a] = bucket.NewBucketDecoder(0, bytes.NewBuffer(data))
	}
	return bucketEntryReader(series, self.Multiplier, decs, attributes), nil
}

// CQLRepository implements the BucketRepository interface
func (self CQLRepository) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		errors <- err
		return
	}
	iter := self.Session.Query("SELECT time, attribs FROM buckets WHERE series = ? and time >= ? and time <= ?", seriesUUID, start, end).Iter()

	var t time.Time
	var attribMap map[string][]byte
	for iter.Scan(&t, &attribMap) {
		reader, err := self.entryReader(series, t, attribMap, attributes)
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

	if err = iter.Close(); err != nil {
		errors <- err
		return
	}
}

// Represents a Cassandra cache
type CQLCache struct {
	Session *gocql.Session
}

// CQLCache implements the SeriesStore interface
func (self *CQLCache) Query(series uuid.UUID, start, end time.Time, attributes []string, entries chan Entry, errors chan error) {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		errors <- err
		return
	}
	iter := self.Session.Query("SELECT time, value FROM cache WHERE series = ? and time >= ? and time <= ?", seriesUUID, start, end).Iter()

	var v float64
	var t time.Time
	for iter.Scan(&t, &v) {
		entries <- Entry{
			Timestamp: t,
			Attributes: map[string]float64{"raw": v},
		}
	}
	err = iter.Close()
	if err != nil {
		errors <- err
	}
}

// CQLCache implements the SeriesStore interface
func (self *CQLCache) Insert(series uuid.UUID, entry Entry) error {
	seriesUUID, err := gocql.UUIDFromBytes(series)
	if err != nil {
		return err
	}
	err = self.Session.Query("INSERT INTO cache (series, time, value) VALUES (?, ?, ?)", seriesUUID, entry.Timestamp, entry.Attributes["raw"]).Exec()
	if err != nil {
		return err
	}
	return nil
}
