#!/bin/bash

echo "--stopping--" >> /tmp/confs.out

for proc in `ps aux | grep hadoop | grep -v 'grep'  | grep -v 'stop.sh' | awk '{} {print $2}'` ;  do 
echo "killing $proc"
kill $proc
done


