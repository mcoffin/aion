#!/usr/bin/env python

from cassandra.cluster import Cluster

cluster = Cluster(['172.28.128.2'])
session = cluster.connect('timedb')

def drop_table(table):
	try:
		session.execute('DROP TABLE ' + table + ';')
	except:
		print('\'' + table + '\' did not exist')

drop_table('cache')
session.execute('CREATE TABLE cache (series uuid, time timestamp, value double, PRIMARY KEY (series, time));')
drop_table('buckets')
session.execute('CREATE TABLE buckets (series uuid, time timestamp, attribs map<text, blob>, PRIMARY KEY (series, time));')
drop_table('tags')
session.execute('CREATE TABLE tags (tag text, value text, series uuid, PRIMARY KEY ((tag, value), series));')
