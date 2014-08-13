aion
======

Cascading time series database with fast tags

## Design

Aion stores data in **levels**. A level represents two things.

1. A strategy for turning input data in to aggregated data. This is called a **filter**. 
   * Example: a passthrough filter is a filter where "aggregated" data is equal to input data
   * Example: A filter could be defined to emit 5min averages of input data.
2. A strategy for storing aggregated data. This is called a **series store**.
   * At the most basic level, this is just a database that stores a time series. On a more complex level, it could be a bucketized storage scheme.

In the Go API, an Aion level is represented by the following data structure.

````go
// A level represents one granularity of data storage in timedb
type Level struct {
	Filter Filter
	Store  SeriesStore
}
````

### Bucketized storage

To cut down on the costs of metadata storage, Aion provides some utilities for defining *series stores* that store compressed data in chunks called **buckets**. The data is compressed using a delta-encoding method passed to a golomb prefix code. All of the data compression can be seen in the `bucket` package.

When a user makes a query on a bucketized level, buckets absent from the persistent storage scheme are either read from a RAM cache or created on the fly, then all the buckets are decoded concurrently, keeping the query time close to `O(1)`.

## Dev Env Setup

For information on setting up a basic Go development environment, see [How to Write Go Code](https://golang.org/doc/code.html)

First, create a `GOPATH`. Your `GOPATH` is effectively a workspace for **all** your go code. A very common GOPATH is `$HOME/go`. Once you have chosen your GOPATH, run the following:

````bash
export GOPATH=<your gopath>
mkdir -p $GOPATH

export PATH=$PATH:$GOPATH/bin
````

After your gopath is set up, you have two options to download the repository.

1. Simply run `go get github.com/FlukeNetworks/aion` to have the go tool automatically attempt to download and install the code for you.
2. Manually clone the repository
   * `mkdir -p $GOPATH/src/github.com/FlukeNetworks && cd $_`
   * `git clone https://github.com/FlukeNetworks/aion.git`

### DynamoDB

Make sure to install the AWS CLI tools and DynamoDB Local. Set the default region for your AWS CLI to be `us-west-1`, then run the `create-tables.sh` script from the repository. After this script has been run, you should be able to properly run the unit tests with `go test`.

### Cassandra

To use the CQL backing stores, you must run a Cassandra cluter/machine. Be sure to edit the desired hostname/IP of the Cassandra machine you want to use. Once you've edited the cluster's information in the correct places, just run `scripts/cassandra recreate` to create the cassandra tables.

### Cayley

In order to use the cayley backing store, you must initialize a Cayley database somewhere, then edit the information of the Cayley database in to the setup routine of the REST api in `aiond/app.go`. This will not be permanent as `aiond` will eventually load all of its configuration from its environment.
