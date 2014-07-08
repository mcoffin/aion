timedb
======

Cascading time series database with fast tags based on Cassandra

# Schema Architecture

TimeDB stores data in cascading **levels**. When data in a level reaches a certain age, it is aggregated, and *cascades* down to the next level. Afterwards, at some point, the data in the first level will expire.

## Data Storage

Each level's data is stored in **buckets**. A bucket contains a window of data differentially encoded. There may be multiple buckets stored for a given time window representing pre-aggregated data.

At the time of the query, both the series and the level at which data is to be queried must be known.

````
CREATE TABLE data (
  series text,
  level int,
  start timestamp,
  baseline double,
  buckets list<blob>,
  PRIMARY KEY ((series, level), start)
);
````

### Value Specification

The values for the *data* table should be as follows:

* series = concatenated string of all tag/value pairs
* level = integer representing the level at which this series is stored
* start = timestamp representing the start time of the bucket
* bucket = see [Bucket Architecture](#Bucket-Architecture)

### Bucket Architecture

TODO

## Tag Storage

Each tag/value pair is stored in a table with a reference to every seriesID that contains that tag/value pair. The table to store the tag/value pairs is formatted as follows:

````
CREATE TABLE tags (
  tag text,
  series text,
  PRIMARY KEY (tag)
);
````

### Value Specification

The values for the *tags* table should be as follows:

* tag = string representing a tag/value pair
* series = series string for the *data* table
  * concatenated string of all tag/value pairs
