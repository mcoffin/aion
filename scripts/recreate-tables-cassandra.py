#!/usr/bin/env python

from cassandra.cluster import Cluster

cluster = Cluster(['172.28.128.2'])
session = cluster.connect('timedb')
try:
	session.execute('DROP TABLE cache;')
except:
	print('\'cache\' did not exist.')
session.execute('CREATE TABLE cache (series uuid, time timestamp, value double, PRIMARY KEY (series, time));')
try:
	session.execute('DROP TABLE buckets;')
except:
	print('\'buckets\' did not exist.')
session.execute('CREATE TABLE buckets (series uuid, time timestamp, attribs map<text, blob>, PRIMARY KEY (series, time));')
