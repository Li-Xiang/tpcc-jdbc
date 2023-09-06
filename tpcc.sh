#!/bin/bash

# ------ Java Environment ------
#JAVA_HOME=/usr/local/jdk-11.0.9.1+1
#JAVA_HOME=/usr/local/jdk1.8.0_251
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
JVM_OPTS="-server -XX:+IgnoreUnrecognizedVMOptions -Xmx4G -Xms1G -XX:MaxMetaspaceSize=256m"

# Always dump on OOM.
OOM_DUMP="-XX:+HeapDumpOnOutOfMemoryError"
JVM_OPTS="${JVM_OPTS} ${OOM_DUMP}"

# ------ JVM GC log Options ------
GC_LOG_FILE="./logs/java_gc_%t.log"

# ---> GC log.
LOG_GC_A9="-Xlog:gc:file=${GC_LOG_FILE}:time"
if [ $MAJOR_JAVA_VER -ge 9 ]; then
  JVM_OPTS="${JVM_OPTS} ${LOG_GC_A9}"
fi

LOG_GC_P9="-Xloggc:%GC_LOG_FILE% -XX:+PrintGC -XX:+PrintGCDateStamps"
if [ $MAJOR_JAVA_VER -lt 9 ]; then
  JVM_OPTS="${JVM_OPTS} ${LOG_GC_A9}"
fi

# ---> GC Details log.
LOG_GCDETAIL_A9="-Xlog:gc*=debug:file=${GC_LOG_FILE}:time"
if [ $MAJOR_JAVA_VER -ge 9 ]; then
  JVM_OPTS="${JVM_OPTS} ${LOG_GCDETAIL_A9}"
fi

LOG_GCDETAIL_P9="-Xloggc:${GC_LOG_FILE} -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
if [ $MAJOR_JAVA_VER -lt 9 ]; then
  JVM_OPTS="${JVM_OPTS} ${LOG_GCDETAIL_P9}"
fi

# ------ JVM GC Options ------
# JAVA 8,9,...,15 with G1GC, JAVA 16+ with ZGC.
G1GC_OPTS="-XX:+UseG1GC -XX:+UseStringDeduplication -XX:MaxGCPauseMillis=100"
ZGC_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"

if [ $MAJOR_JAVA_VER -ge 8 ] && [ $MAJOR_JAVA_VER -lt 16 ]; then
  JVM_OPTS="${JVM_OPTS} ${G1GC_OPTS}"
else 
  JVM_OPTS="${JVM_OPTS} ${ZGC_OPTS}"
fi

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 {benchmark|load|drop|addfk|dropfk|check} {benchmark-config-file} " 
  exit 1
fi

BENCHMARK_CFG=$1
BENCHMARK_CMD=$2

eval ${JAVACMD} ${JVM_OPTS} -cp \"${APP_LIB}\" ${MAIN_CLASS} ${BENCHMARK_CMD} ${BENCHMARK_CFG}
