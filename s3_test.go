package timedb

import (
    "math"
    "testing"
    "time"
    "launchpad.net/goamz/aws"
    "launchpad.net/goamz/s3"
)

func TestS3BucketStore(t *testing.T) {
    auth, err := aws.EnvAuth()
    if err != nil {
        t.Fatal(err)
    }
    myS3 := s3.New(auth, aws.USWest)
    bucket := myS3.Bucket("timedb-dev")
    store := &S3BucketStore{
        BucketStore{
            Duration: 60 * time.Second,
            Granularity: 0,
            Aggregations: []string{"min", "max", "avg"},
            Multiplier: math.Pow10(1),
        },
        bucket,
    }
    store.Storer = store
    testQueryLevel(store, t)
}

func BenchmarkS3Get(b *testing.B) {
    auth, err := aws.EnvAuth()
    if err != nil {
        b.Fatal(err)
    }
    myS3 := s3.New(auth, aws.USWest)
    bucket := myS3.Bucket("timedb-dev")
    store := &S3BucketStore{
        BucketStore{
            Duration: 60 * time.Second,
            Granularity: 0,
            Aggregations: []string{"min", "max", "avg"},
            Multiplier: math.Pow10(1),
        },
        bucket,
    }
    store.Storer = store
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := bucket.Get(store.s3BlockPath(testSeriesUUID, testStart))
        if err != nil {
            b.Fatal(err)
        }
    }
}
