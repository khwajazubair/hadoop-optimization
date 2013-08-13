#!/bin/bash



cd ${1}
mkdir temp
declare -a header=('job_ID             ' 'start_time ' 'end_time' 'job_d' 'mp' 'rd' ' r_bytes  ' 'w_bytes  ' 'mp_t  ' ' rd_t ' '?' ' cpu_t' 
					' mem_amnt ' 'fs' 'kt' 's' 'sp' 'rn' );

echo -e "${header[@]}"> ../$2


for i in 1 2 3 4 5
do	
	dir1="dir${i}" 
	
	#find directory
	dir1=$(find -maxdepth 1 -type d -name '*capacity-spec-true*'| awk 'NR==1')
	
	IFS='/' b_split=($dir1)
	for p in ${b_split[@]}
	do
    		dirA="$p"
	done
	
	echo ${dirA}
	cd ${dirA}
	
	#fine file inside directory
	file=$(find . -name 'logs_summ*')
	IFS='/' file_split=($file)
		for s in ${file_split[@]}
			do
    		file1="$s"
		done

	#write file lines to temporary file container
	cat $file1 | while read LINE; do
		echo "${LINE} cp t $i">>../temp.csv

	done
	cd ..
	#move folder to temp
	mv ${dirA} temp/
		
done








for i in 1 2 3 4 5
do	
	dir1="dir${i}" 
	
	#find directory
	dir1=$(find -maxdepth 1 -type d -name '*capacity-spec-false*'| awk 'NR==1')
	
	IFS='/' b_split=($dir1)
	for p in ${b_split[@]}
	do
    		dirA="$p"
	done
	
	echo ${dirA}
	cd ${dirA}
	
	#fine file inside directory
	file=$(find . -name 'logs_summ*')
	IFS='/' file_split=($file)
		for s in ${file_split[@]}
			do
    		file1="$s"
		done

	#write file lines to temporary file container
	cat $file1 | while read LINE; do
		echo "${LINE} cp f $i">>../temp.csv

	done
	cd ..
	#move folder to temp
	mv ${dirA} temp/
		
done











for i in 1 2 3 4 5
do	
	dir1="dir${i}" 
	
	#find directory
	dir1=$(find -maxdepth 1 -type d -name '*fair-spec-true*'| awk 'NR==1')
	
	IFS='/' b_split=($dir1)
	for p in ${b_split[@]}
	do
    		dirA="$p"
	done
	
	echo ${dirA}
	cd ${dirA}
	
	#fine file inside directory
	file=$(find . -name 'logs_summ*')
	IFS='/' file_split=($file)
		for s in ${file_split[@]}
			do
    		file1="$s"
		done

	#write file lines to temporary file container
	cat $file1 | while read LINE; do
		echo "${LINE} fs t $i">>../temp.csv

	done
	cd ..
	#keep copy of folder and move folder to temp
	mv ${dirA} temp/
		
done






for i in 1 2 3 4 5
do	
	dir1="dir${i}" 
	
	#find directory
	dir1=$(find -maxdepth 1 -type d -name '*fair-spec-false*'| awk 'NR==1')
	
	IFS='/' b_split=($dir1)
	for p in ${b_split[@]}
	do
    		dirA="$p"
	done
	
	echo ${dirA}
	cd ${dirA}
	
	#fine file inside directory
	file=$(find . -name 'logs_summ*')
	IFS='/' file_split=($file)
		for s in ${file_split[@]}
			do
    		file1="$s"
		done

	#write file lines to temporary file container
	cat $file1 | while read LINE; do
		echo "${LINE} fs f $i">>../temp.csv

	done
	cd ..
	#keep copy of folder and move folder to temp
	mv ${dirA} temp/
		
done






cat temp.csv | tr -d '\b\r'>>../$2
chmod a+rwx ../$2
mv temp/* .
rm -r temp
rm -r temp.csv