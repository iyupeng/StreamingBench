#!/bin/bash

curDir=$(cd  `dirname $0`;pwd)
rootDir=$(dirname $curDir)

if [ -e $rootDir/conf/env ]; then
    source $rootDir/conf/env
fi

mainClass=com.intel.streaming_benchmark.flink.Benchmark
dataGenClass=com.intel.streaming_benchmark.Datagen

for sql in `cat $rootDir/conf/queriesToRun`;
do
    echo "Data generator start!"
    for host in `cat $rootDir/conf/dataGenHosts`;do ssh $host "sh $rootDir/utils/dataGenerator.sh $DATAGEN_TIME $THREAD_PER_NODE $sql flink"; done
    echo "RUNING $sql"
    mkdir -p $rootDir/flink/log
    nohup $FLINK_HOME/bin/flink run -c $mainClass $rootDir/flink/target/flink-1.0-SNAPSHOT.jar $CONF $sql >> $rootDir/flink/log/${sql}.log 2>&1 &
    sleep $DATAGEN_TIME
    FLINK_ID=`"$FLINK_HOME/bin/flink" list | grep "$sql" | awk '{print $4}'; true`
    PID=`$FLINK_HOME/bin/flink stop -p $rootDir/flink/result/check_${sql} -d  $FLINK_ID`
echo $FLINK_ID
done

