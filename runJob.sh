#!/bin/bash
CURPATH=$(dirname "$0")
cd $CURPATH

hdfs dfs -rm -r /output
hadoop jar src/ngram.jar Driver input /output 3 3