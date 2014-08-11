#!/usr/bin/env python

from cassandra.cluster import Cluster

cluster = Cluster(['172.28.128.2'])
session = cluster.connect('timedb')
session.execute('DROP TABLE cache;')
session.execute('CREATE TABLE cache (series uuid, time timestamp, value double, PRIMARY KEY (series, time));')
