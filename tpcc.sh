#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 {benchmark|load|drop|addfk|dropfk|check|gather} {benchmark-config-file} "
  exit 1
fi

# ------ Java Environment ------
#JAVA_HOME=/usr/local/jdk-11.0.9.1+1
#JAVACMD=$JAVA_HOME/bin/java
JAVACMD=java

PRG="$0"
while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
  PRG="$link"
  else
  PRG=`dirname "$PRG"`"/$link"
  fi
done

APP_HOME=`dirname "$PRG"`
APP_LIB="${APP_HOME}/*:.:${APP_HOME}/lib/*:${APP_HOME}/modules/*"
MAIN_CLASS=org.littlestar.tpcc.Benchmark

mkdir -p ${APP_HOME}/logs
cd $APP_HOME

#
JAVA_VER_STR=$("$JAVACMD" -version 2>&1 | awk -F[\"\-] '/version/ {print $2}')
MAJOR_JAVA_VER="${JAVA_VER_STR%%.*}"

if [ ${MAJOR_JAVA_VER} -eq 1 ]; then
  MAJOR_JAVA_VER=$(awk -F[\"\.] '{print $2}' <<< ${JAVA_VER_STR})
fi

# ------ Java Version Check ------
if [ $MAJOR_JAVA_VER -lt 11 ]; then
  echo $0: Java version '$JAVA_VER_STR' is too low, needs Java 11 or later.
  exit 1
fi

# ------ JVM Options ------
JVM_OPTS="-server -Dfile.encoding=UTF-8 -XX:+UseStringDeduplication"
#JVM_OPTS="${JVM_OPTS} -XX:+PrintCommandLineFlags"


# ------ Always dump on OOM. ------
OOM_DUMP="-XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./logs/"
JVM_OPTS="${JVM_OPTS} ${OOM_DUMP}"


# ------- Memory Options -----
MEM_OPTS="-Xms2048m -Xmx2048m -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=512m "
JVM_OPTS="${JVM_OPTS} ${MEM_OPTS}"


# ------ JVM GC Options ------
GC_OPTS="-XX:+UseZGC"
#GC_OPTS="-XX:+UseG1GC"
JVM_OPTS="${JVM_OPTS} ${GC_OPTS}"

# ------ JVM GC log Options ------
GC_LOG_FILE="./logs/java_gc_%t.log"

if [ $MAJOR_JAVA_VER -ge 9 ]; then
  #GC_LOG_OPTS="-Xlog:safepoint=info,gc*=debug:file=${GC_LOG_FILE}:time,pid:filecount=4,filesize=20M"
  GC_LOG_OPTS="-Xlog:safepoint=info,gc*=info:file=${GC_LOG_FILE}:time,pid:filecount=4,filesize=20M"
else
  GC_LOG_OPTS="-XX:+PrintGCDetails"
  GC_LOG_OPTS="${GC_LOG_OPTS} -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDateStamps"
  GC_LOG_OPTS="${GC_LOG_OPTS} -Xloggc:${GC_LOG_FILE}"
  GC_LOG_OPTS="${GC_LOG_OPTS} -XX:+UseGCLogFileRotation -XX:GCLogFileSize=20M -XX:NumberOfGCLogFiles=4"
fi
#JVM_OPTS="${JVM_OPTS} ${GC_LOG_OPTS}"



BENCHMARK_CFG=$1
BENCHMARK_CMD=$2

eval ${JAVACMD} ${JVM_OPTS} -cp \"${APP_LIB}\" ${MAIN_CLASS} ${BENCHMARK_CMD} ${BENCHMARK_CFG}