timedb
======

Cascading time series database with fast tags based on Cassandra

# Architecture

Data in timedb is stored in **query levels**. A query level contains blocks of differentially encoded data over a time span. Each of these blocks of data is called a **bucket**. Ideally, a query level is of a duration such that when a bucket is queried out, the entire bucket of data was desired in the query. If the entire bucket is used for the query, then no additional data transfer overhead was added by transferring the entire bucket instead of just the required data.

Additionally, a query level can contain multiple **aggregation levels**. These allow the storage of data at different granularities within the query level.

The following struct is used to contain information about a query level and its aggregation levels.

````go
type BucketStore struct {
    Duration time.Duration
    Granularities []time.Duration
    Aggregations []string
}
````

# Data Flow

As data comes in to timedb, it is first *cached* in a non-aggregated query level. This default query level has no additional granularities/aggregations, and only contains raw data. Later, the data in the cached query level is "rolled up" and aggregated for storage in the next query level. After another period of time, the data may roll further down into another query level.

##Queries

When a query is made against timedb, a desired granularity must be given, and the system will search for data of the closest granularity it can find, then query against the level that contains that granularity.
