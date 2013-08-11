#!/bin/bash

#cd ${1}
#echo ${1}

cd ${1}
#mkdir test
cp -r test/* .
echo "">../exp_2_bs.csv

for i in 1 2 3 4 5

do
	
	dir1="dir${i}" 
	#echo ${dir1}
	dir1=$(find -maxdepth 1 -type d -name '*capacity-spec-true*'| awk 'NR==1')
	echo ${dir1}
	IFS='/' b_split=($dir1)
	for p in ${b_split[@]}
	do
    		dirA="$p"
	done
	cd ${dirA}
	echo "`ls`"

	#fileLines=`cat logs_summ*`
	#echo "$fileLines"
	set -f 
	IFS='
	'	
	for line in $(cat "logs_summ"*)
	 do 

			echo "${line} cp t $i">>../exp_2_bs.csv
	done
	unset f
	cd ..
	echo "copying folder ${dirA} to test"
	cp -r ${dirA} test/
	rm -r ${dirA}
	
done
cat exp_2_bs.csv | tr -d '\b\r'>../test.csv
