#!/bin/bash
if [ $# -ne 6 ]; then
  echo "Usage: $0   <num_train_data> <dim_feature> <num_classes> <num_partitions> <save_dir> <partition_id>"
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
$prog_path $1 $2 $3 $4 `readlink -f $5` $6
