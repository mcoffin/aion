timedb
======

Cascading time series database with fast tags based on Cassandra

# Architecture

Data in timedb is stored in **query levels**. A query level contains blocks of differentially encoded data over a time span. Each of these blocks of data is called a **bucket**. Ideally, a query level is of a duration such that when a bucket is queried out, the entire bucket of data was desired in the query. If the entire bucket is used for the query, then no additional data transfer overhead was added by transferring the entire bucket instead of just the required data.

Additionally, a query level can contain multiple **aggregation levels**. These allow the storage of data at different granularities within the query level.

Information about each query level is stored in the following structs:

````go
type QueryLevel struct {
    Duration time.Duration
    Aggregations []AggregationLevel
}

type AggregationLevel struct {
    Period time.Duration
}
````

# Data Flow

As data comes in to timedb, it is first *cached* in a default query level.
