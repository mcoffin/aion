#!/bin/bash

create_table () {
	aws dynamodb create-table \
		--table-name $1 \
		--attribute-definitions AttributeName=series,AttributeType=S AttributeName=time,AttributeType=N \
		--key-schema AttributeName=series,KeyType=HASH AttributeName=time,KeyType=RANGE \
		--provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=10 \
#		--endpoint-url http://localhost:8000
	return 0
}

create_table timedb
create_table timedb-bucket
