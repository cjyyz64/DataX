#!/bin/bash
<<::
--CASEID--
$Id
--TEST--
[DataX][ZsearchWriter] 只有基本参数，读写正常
--DESC--
DataX - ZsearchWriter Test
--PRI--

--TAGS--

--EXPECTREGEX--
PASS$
--FILE--
::

home_dir=`echo $0 |/usr/bin/xargs /usr/bin/dirname`
[ ${home_dir} == "." ] && home_dir=`/bin/pwd`
datax_run_scrpit="/home/admin/datax3/bin/datax.py"

source ${home_dir}/../global_env.sh

echo "datax run ..............."
/usr/local/bin/python ${datax_run_scrpit} ${home_dir}/job.json > process.log 2>&1

grep "100" ${home_dir}/process.log>dirty_log
if [ $? -ne 0 ]
then
     echo "Failed"
     exit -1
else
     echo "PASS"
     exit 0
fi

