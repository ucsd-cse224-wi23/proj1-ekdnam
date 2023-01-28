#!/bin/bash

##Remove old netsort executable
rm -f netsort

##Build netsort
go build -o netsort netsort.go

##Run for process
TESTCASE='1'
for i in $(seq 0 3)
do
  SERVER_ID=$i
  INPUT_FILE_PATH='testcases/testcase'${TESTCASE}'/input-'${SERVER_ID}'.dat'
  OUTPUT_FILE_PATH='testcases/testcase'${TESTCASE}'/output-'${SERVER_ID}'.dat'
  CONFIG_FILE_PATH='testcases/testcase'${TESTCASE}'/config.yaml'
  ./netsort ${SERVER_ID} ${INPUT_FILE_PATH} ${OUTPUT_FILE_PATH} ${CONFIG_FILE_PATH} &
done

wait
