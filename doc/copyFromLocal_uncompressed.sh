#  Louai Alarabi - 02/15/2016
# This script check all the gzip file recursively in a folder then do the following 
# 1) decompress the file.gz 
# 2) copy the decompressed file to HDFS of hadoop 
# 3) compress the file again to gz
# 4) Finally after copy all the files it start the hadoop job
#
/export/scratch/louai/hadoop/hadoop-2.7.2/bin/hadoop fs -mkdir /GeotaggedSample
echo " Check files in the target directory... "
files=$(/export/scratch/louai/hadoop/hadoop-2.7.2/bin/hadoop fs -ls /GeotaggedSample/)
for i in $(find `pwd` -name "*.gz");
do
	filename=$(basename "$i" | sed 's/.gz//g')
	echo "$filename"
	if echo "$files" | grep "$filename";then
		echo " $filename exist"
	else
		echo "$filename miss"
		echo "decompress file $i"
		gzip -d "$i"
		echo "copy file to the hdfs $i"
		/export/scratch/louai/hadoop/hadoop-2.7.2/bin/hadoop fs -copyFromLocal "${i%.gz}" /GeotaggedSample/
		echo "compress again $i "
		gzip "${i%.gz}"
	fi

done 

echo "starting hadoop job"
/export/scratch/louai/hadoop/hadoop-2.7.2/bin/hadoop jar /export/scratch/louai/analyzeMapRed-1-withlib.jar /GeotaggedSample /unsortedkeywords /sortedkeyword /quadtree_mbrs.txt keyword
/export/scratch/louai/hadoop/hadoop-2.7.2/bin/hadoop jar /export/scratch/louai/analyzeMapRed-1-withlib.jar /GeotaggedSample /usortedtime /sortedtime /quadtree_mbrs.txt time      


