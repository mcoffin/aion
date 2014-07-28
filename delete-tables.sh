#!/bin/bash

delete_table () {
	aws dynamodb delete-table \
		--table-name $1
}

delete_table timedb
delete_table timedb-bucket
