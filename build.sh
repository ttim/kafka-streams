#!/bin/sh

# configure Java paths
unset JAVA_TOOL_OPTIONS
export JAVA_HOME=/usr/lib/jvm/java-1.8.0
JAVA=$JAVA_HOME/bin/java
JAVA_CC=$JAVA_HOME/bin/javac

CLASSPATH=.:"libs/*"
export CLASSPATH

echo --- Cleaning
rm -f *.class

echo --- Compiling Java
$JAVA_CC *.java
if [ $? -eq 0 ]
then
  echo "Success..."
else
  echo "Error..."
  exit 1
fi

