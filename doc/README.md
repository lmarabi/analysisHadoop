Louai Alarabi - last updated: 16/02/2016

Main idea 
=========
This analysisHadoop projects process the compressed geotaged tweets based on quadtree leaves mbrs. This projects contains two map-reduce jobs, the first mapred job extract words from the tweets and stores keyword and counts of the each keywords. The second mapred job basically combines the keywords that are associated with a rectangular boundary into a single line. To elaborate more about the details of each job, please see the following sections. 

**1st Map-Red**
------
*Input:* A text line that represent a tweet in a JASON format. 
*output:* a line that conststs of <mbr,keyword,count>



*2nd Map-Red*
---------
*Input:* basically the output from the first map-red job. 
*Ouput:* The output will be a tab separated file. Each line consist of <MBR date [keyword,count]+>, the mbr represents the leaf boundary of the quadtree, the date represent the time, and the one or more keywords which consist of keyword and count. 




How to use:
==========
1) build the jar using ant 
2) Put the compressed dataset to a folder as is.
3) Place the script that "decompress, copy, and compress" It will run automatically, Note that to check the path of hadoop in the script. 
