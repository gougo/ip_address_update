#!/bin/bash
ource /home/kpi/.bash_profile
cd `dirname $0`
if [ "x" == "x${1}" ]
then
    echo "no args.please exec like: sh x.sh 2014091101 " 1>&2    
    exit 1
fi 
year=${1::4}
month=${1:4:2}
day=${1:6:2}
hour=${1:8:2}
cd /mnt/mfs/stat/recent/dir_ip_src
sort -t'`' -k1n result_${year}${month}${day}* |uniq > ip_uniq_${year}${month}${day}
python read_sort_ipfile.py ip_uniq_${year}${month}${day} ip_merge_${year}${month}${day}

