#!/bin/bash
CURPATH=$(dirname "$0")
cd $CURPATH

cd src/
hadoop com.sun.tools.javac.Main *.java
jar cf ngram.jar *.class