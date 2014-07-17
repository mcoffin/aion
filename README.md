timedb
======

Cascading time series database with fast tags

# Architecture

Data in timedb is stored in **query levels**. A query level represents a scheme from both inserting and querying data from some kind of storage.

Each query level must implement the following interface.

````go
type QueryLevel interface {
    Insert(entries chan Entry, series uuid.UUID, success chan error)
    Query(entries chan Entry, series uuid.UUID, aggregation string, start time.Time, end time.Time, success chan error)
}
````

## Common Query Levels

While implementations can define any kind of query level they desire, there are two very common kinds of query levels: the **cache**, and a number of **bucketized levels**. As data comes in to timedb, it will be stored in the cache. When enough data has accumulated there, it is rolled up to the first bucketized level. When enough data accumulates in that level, it will be rolled up in to the next level, and so on and so forth.

this is the aspect of timedb that is considered to be **cascading**.

### Cache

The first query level in timedb is often some kind of a cache. The cache takes in raw data points, and stores them as is. This allows for fast access to the data, and provides interim storage until the data can be archived into a bucketized level.

### Bucketized Levels

After enough data exists in some previous query level, data can be *rolled up* in to bucketized levels.

A bucketized level contains blocks of differentially encoded data over a time span. All blocks within a bucketized level have the same **duration**, and all data has a maximum **granularity**. For example, a bucketized level might be defined to keep 1min granularity data in 2hr blocks.

Each block contains multiple **aggregations** of finer data. In the above example, a block that contains 1min granularity data in 2hr blocks might contain the `min`, `max`, and `avg` of the data for each minute.

Each bucketized level can be configured by the following struct.

````go
type BucketStore struct {
    Duration time.Duration
    Granularity time.Duration
    Aggregations []string
    Multiplier float64
    Storer BucketStorer
}
````
