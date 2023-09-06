@echo off
setlocal

REM ------ Java Environment ------
REM set JAVA_HOME=D:\Program Files\Java\jdk1.8.0_261
set JAVA_HOME=D:\Program Files\jdk-11.0.14.1+1
set PATH=%JAVA_HOME%\bin;%PATH%

set APP_HOME=%~dp0
set APP_LIB="%APP_HOME%*;.;%APP_HOME%lib\*"
set MAIN_CLASS=org.littlestar.tpcc.Benchmark
cd %APP_HOME%

if not exist "%APP_HOME%\logs" md "%APP_HOME%\logs"

for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    set JAVA_VER_STR=%%g
)

set JAVA_VER_STR=%JAVA_VER_STR:"=%

for /f "delims=. tokens=1-3" %%v in ("%JAVA_VER_STR%") do (
	if "%%v"=="1" (
		set MAJOR_JAVA_VER=%%w 
	) else (
		set MAJOR_JAVA_VER=%%v
	)
)

REM ------ Java Version Check ------
if %MAJOR_JAVA_VER% LSS 11 (
  echo %0: Java version '%JAVA_VER_STR%' is too low, needs Java 11 or later.
  goto :eof
)

REM ------ JVM Options ------
set JVM_OPTS=-server -XX:+IgnoreUnrecognizedVMOptions -Xmx4G -Xms1G -XX:MaxMetaspaceSize=256m

REM : Always dump on OOM.
set OOM_DUMP=-XX:+HeapDumpOnOutOfMemoryError
set JVM_OPTS=%JVM_OPTS% %OOM_DUMP%

REM ------ JVM GC log Options ------
set GC_LOG_FILE=./log/java_gc_%%t.log

REM -> GC log.
REM set LOG_GC_A9=-Xlog:gc:file=%GC_LOG_FILE%:time
REM if %MAJOR_JAVA_VER% GEQ 9 (
REM   set JVM_OPTS=%JVM_OPTS% %LOG_GC_A9%
REM )
REM 
REM set LOG_GC_P9=-Xloggc:%GC_LOG_FILE% -XX:+PrintGC -XX:+PrintGCDateStamps
REM if %MAJOR_JAVA_VER% LSS 9 (
REM   set JVM_OPTS=%JVM_OPTS% %LOG_GC_P9%
REM )

REM -> GC Details log.
REM set LOG_GCDETAIL_A9=-Xlog:gc*=debug:file=%GC_LOG_FILE%:time
REM if %MAJOR_JAVA_VER% GEQ 9 (
REM   set JVM_OPTS=%JVM_OPTS% %LOG_GCDETAIL_A9%
REM )
REM 
REM set LOG_GCDETAIL_P9=-Xloggc:%GC_LOG_FILE% -XX:+PrintGCDetails -XX:+PrintGCDateStamps
REM if %MAJOR_JAVA_VER% LSS 9 (
REM  set JVM_OPTS=%JVM_OPTS% %LOG_GCDETAIL_P9%
REM )


REM ------ JVM GC Options ------
REM JAVA 8,9,...,15 with G1GC.
set G1GC_OPTS=-XX:+UseG1GC -XX:+UseStringDeduplication -XX:MaxGCPauseMillis=100
if %MAJOR_JAVA_VER% GEQ 8 if %MAJOR_JAVA_VER% LSS 16 (
  set JVM_OPTS=%JVM_OPTS% %G1GC_OPTS%
)

set USAGE="usage: %0 {benchmark|load|drop|addfk|dropfk|check} {benchmark-config-file} "

if "%1"=="" (
 echo %USAGE%
  exit /b
)

if "%2"=="" (
  echo %USAGE%
  exit /b
)
set BENCHMARK_CFG=%1
set BENCHMARK_CMD=%2
java %JVM_OPTS% -cp %APP_LIB% %MAIN_CLASS% %BENCHMARK_CMD% %BENCHMARK_CFG% 
