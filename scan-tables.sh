#!/bin/bash

scan_table () {
	aws dynamodb scan \
		--table-name $1 \
		--endpoint-url http://localhost:8000
}

echo "timedb"
scan_table timedb
echo "timedb-bucket"
scan_table timedb-bucket
