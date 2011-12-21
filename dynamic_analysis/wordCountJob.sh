#!/bin/bash

export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/1.6.0/Home
HADOOP=/Users/asrabkin/workspace/hadoop-0.20.2

#HADOOP=/Users/asrabkin/Documents/cloudera/hadoop-0.20.2-cdh3u0
CONFDIR=dyn_t_conf

echo "JAVA_HOME is $JAVA_HOME"
TESTNAME=MapRed-WC
echo "Hadoop dir is $HADOOP confdir is $CONFDIR"

echo "Clearing Hadoop log dir"
rm -r $HADOOP/logs
rm -rf /tmp/hadoop-asrabkin


if !(ps aux | grep namenode | grep -vq 'grep')
then
  echo "No Hadoop HDFS running. Formatting FS..."
  $HADOOP/bin/hadoop --config $CONFDIR namenode -format > $TESTNAME-nn_format.log 2>&1
  echo "Format done. Starting nodes."
  ($HADOOP/bin/hadoop --config $CONFDIR namenode > $TESTNAME-nn.log 2>&1) &
  ($HADOOP/bin/hadoop --config $CONFDIR datanode > $TESTNAME-dn.log 2>&1) &
  ($HADOOP/bin/hadoop --config $CONFDIR secondarynamenode > $TESTNAME-2nn.log 2>&1) &

  sleep 10

else
  echo "Namenode apparently running. yay!"
fi

date
echo "starting JT and TT; expect a 20 second pause. Screen output is from client and will also go to file."

($HADOOP/bin/hadoop --config $CONFDIR jobtracker > $TESTNAME-jt.log 2>&1) &
($HADOOP/bin/hadoop --config $CONFDIR tasktracker > $TESTNAME-tt.log 2>&1) &
$HADOOP/bin/hadoop --config $CONFDIR fs -copyFromLocal /etc/shells /tmpshells 2>&1 | tee -a $TESTNAME-client.log 

sleep 20

if !(ps aux | grep JobTracker | grep -vq 'grep'); then
echo "JT didn't start"
exit 1
fi

if !(ps aux | grep TaskTracker | grep -vq 'grep'); then
echo "TT didn't start"
exit 1
fi

date
echo "Submitting job; allowing 60 seconds to run"
HADOOPEXAMPLE=`ls $HADOOP/hadoop-*example*.jar`
$HADOOP/bin/hadoop --config $CONFDIR jar $HADOOPEXAMPLE wordcount /tmpshells /tout > $TESTNAME-submit.log 2>&1

sleep 60

cat `find $HADOOP/logs/userlogs -name syslog` > $TESTNAME-userlogs.log

for proc in `ps aux | grep hadoop | grep -v 'grep' | grep -vi python | grep -v chord  | awk '{} {print $2}'` ;  do 
echo "killing $proc"
kill $proc
done


#exit 0
