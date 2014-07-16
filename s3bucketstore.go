package timedb

import (
    "launchpad.net/goamz/s3"
)

type S3BucketStore struct {
    BucketStore
    Bucket *s3.Bucket
}
