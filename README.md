timedb
======

Cascading time series database with fast tags based on Cassandra

# Schema Architecture

TimeDB stores data in cascading **levels**. When data in a level reaches a certain age, it is aggregated, and *cascades* down to the next level. Afterwards, at some point, the data in the first level will expire.

Each level's data is stored in **buckets**. A bucket contains a window of data differentially encoded. The buckets are then written to the database in a table as follows:

````
CREATE TABLE data (
  series text,
  level int,
  start timestamp,
  bucket blob,
  PRIMARY KEY ((series, level), start)
);
````

## Value Specification

The values for the *data* table should be as follows:

* series = concatenated string of all tag/value pairs
* level = integer representing the level at which this series is stored
* start = timestamp representing the start time of the bucket
* bucket = see [Bucket Architecture](#Bucket-Architecture)

## Bucket Architecture

TODO
