#!/bin/bash
# (c) Copyright 2011 the Regents of the University of California
# Portions (c) Copyright 2011 Cloudera, Inc.
#  See the file COPYING for license information


export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Home
#HADOOP=/Users/asrabkin/workspace/hadoop-0.20.2/

HADOOP=/Users/asrabkin/Documents/cloudera/hadoop-0.20.2-cdh3u0
CONFDIR=dyn_t_conf

echo "JAVA_HOME is $JAVA_HOME"
TESTNAME=MapRed-Idle

if !(ps aux | grep namenode | grep -vq 'grep')
then
	rm -rf /tmp/hadoop-asrabkin
  echo "No Hadoop HDFS running. Starting it."
  $HADOOP/bin/hadoop --config $CONFDIR namenode -format
  ($HADOOP/bin/hadoop --config $CONFDIR namenode > $TESTNAME-nn.log 2>&1) &
  ($HADOOP/bin/hadoop --config $CONFDIR datanode > $TESTNAME-dn.log 2>&1) &
  sleep 10

else
  echo "Namenode apparently running. yay!"
fi

echo "starting JT and TT"

($HADOOP/bin/hadoop --config $CONFDIR jobtracker > $TESTNAME-jt.log 2>&1) &
($HADOOP/bin/hadoop --config $CONFDIR tasktracker > $TESTNAME-tt.log 2>&1) &
sleep 10

if !(ps aux | grep JobTracker | grep -vq 'grep'); then
echo "JT didn't start"
exit 1
fi

if !(ps aux | grep TaskTracker | grep -vq 'grep'); then
echo "TT didn't start"
exit 1
fi

for proc in `ps aux | grep hadoop | grep -v 'grep'  | grep -v 'stop.sh' | awk '{} {print $2}'` ;  do 
echo "killing $proc"
kill $proc
done


exit 0
