#!/bin/bash

JAR=$1
shift

MAIN_CLASS=$1
shift

ARGS=$*

mvn clean package
mvn dependency:copy-dependencies 
java -cp "target/dependency/*:target/${JAR}" ${MAIN_CLASS} ${ARGS}
