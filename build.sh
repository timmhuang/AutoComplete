#!/bin/bash
CURPATH=$(dirname "$0")
cd $CURPATH

cd src
hadoop com.sun.tools.javac.Main *.java -d ../build/
cd ../build
jar cf ngram.jar *.class