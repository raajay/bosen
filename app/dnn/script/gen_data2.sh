#!/bin/bash
if [ $# -ne 7 ]; then
  echo "Usage: $0   <num_train_data> <dim_feature> <num_classes> <num_workers> <save_dir> <partition_st> <number_runs>"
  exit
fi
progname=gen_data2
script_path=`readlink -f $0`
script_dir=`dirname $script_path`
project_root=`dirname $script_dir`
prog_path=$project_root/bin/$progname
if [ ! -d $5 ]; then
  mkdir $5
fi

START=$(($6))
END=$(($6 + $7 - 1))

TOTAL_PARTITIONS=$(($4 * $7))

for i in {$START..$END }
do
    $prog_path $1 $2 $3 ${TOTAL_PARTITIONS} `readlink -f $5` $i
done
