#!/bin/bash
SCRIPT_PATH=$(dirname $0)

$SCRIPT_PATH/delete-tables-dynamo.sh
sleep 1
$SCRIPT_PATH/create-tables-dynamo.sh
