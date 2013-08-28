#!/bin/bash


presentDir=`pwd`
cd $1

dirs=`ls -d -- */`
for dir in ${dirs};
do 
cd ${dir}
currentDir=`pwd`
#echo ${currentDir}
#rm -r jobs.out
python /home/zubair/hadoop-optimization/runs/job_corrector.py ${currentDir}
cd ..
done