#!/bin/bash
CURPATH=$(dirname "$0")
cd $CURPATH

hadoop com.sun.tools.javac.Main src/*.java -d build/
jar cf build/ngram.jar build/*.class