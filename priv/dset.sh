#!/bin/bash
# $1 - line
# $2 - size 

rm -f /tmp/block.dat
for i in `seq 1 1000` ; do cat $1 >> /tmp/block.dat; done
for i in `seq 1   $2` ; do cat /tmp/block.dat; done
rm -f /tmp/block.dat
  
