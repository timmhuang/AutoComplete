#!/bin/bash
CURPATH=$(dirname "$0")
cd $CURPATH

cd build/
hdfs dfs -rm -r /output
hadoop jar ngram.jar Driver input /output 4 4