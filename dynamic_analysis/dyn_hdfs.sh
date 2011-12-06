#!/bin/bash
# (c) Copyright 2011 the Regents of the University of California
# Portions (c) Copyright 2011 Cloudera, Inc.
#  See the file COPYING for license information


export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Home

#HADOOP=/Users/asrabkin/workspace/hadoop-0.20.2/
HADOOP=/Users/asrabkin/Documents/cloudera/hadoop-0.20.2-cdh3u0

CONFDIR=dyn_t_conf

HADOOP_TMP_DIR=/tmp/hadoop-asrabkin
TESTNAME=HDFS-mini

rm -r $HADOOP_TMP_DIR

echo "running Hadoop"
$HADOOP/bin/hadoop --config $CONFDIR namenode -format > $TESTNAME-nn_format.log  2>&1

($HADOOP/bin/hadoop --config $CONFDIR namenode > $TESTNAME-nn.log 2>&1) &

($HADOOP/bin/hadoop --config $CONFDIR datanode > $TESTNAME-dn.log 2>&1) &

sleep 5
echo "------Starting --- "
sleep 3

for (( i=0;i<10;i++)); do
	echo "---- doing a put ----" 
	$HADOOP/bin/hadoop --config $CONFDIR fs -copyFromLocal /etc/shells /tmpshells > $TESTNAME-client.log 2>&1
	$HADOOP/bin/hadoop --config $CONFDIR fs -ls / >> $TESTNAME-client.log 2>&1
	sleep 1
	$HADOOP/bin/hadoop --config $CONFDIR fs -rm /tmpshells >> $TESTNAME-client.log 2>&1
done

for proc in `ps aux | grep hadoop | grep -v 'grep'  | grep -v 'stop.sh' | awk '{} {print $2}'` ;  do 
echo "killing $proc"
kill $proc
done

echo "done"

#grep -o 'Config Monitoring.*' hadoop.log | grep -o '[^ ]* at' | cut -f 1 -d ' ' | sort | uniq -c > used_in_toyhdfs.txt
#python summarize-dynlogs.py $TESTNAME

exit 0
