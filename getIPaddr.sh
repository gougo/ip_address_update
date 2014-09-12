#!/bin/bash
source /home/kpi/.bash_profile
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
python gaode_ip_new.py /mnt/mfs/stat/recent/dir_ip_src/uslog_${year}-${month}-${day}_${hour}_addcn /mnt/mfs/stat/recent/dir_ip_src/result_${year}${month}${day}${hour}
