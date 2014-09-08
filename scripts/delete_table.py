from boto.dynamodb2.layer1 import DynamoDBConnection

conn = DynamoDBConnection(
		host='localhost',
		port='8000',
		is_secure=False)
conn.delete_table('aion')
