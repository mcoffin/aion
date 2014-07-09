timedb
======

Cascading time series database with fast tags based on Cassandra

# Architecture

Data in timedb is stored in **query levels**. Each query level contains blocks of data blocked together at a time interval that represents a common time interval over which to query.

Within a query level, each block of data (or **bucket**), is stored with a start timestamp and base value, and the data accross the time range is differentially encoded.
