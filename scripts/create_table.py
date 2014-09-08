from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection

conn = DynamoDBConnection(
		host='localhost',
		port='8000',
		is_secure=False)


tbl = Table.create('aion', schema=[
	HashKey('series')],
	connection=conn)
