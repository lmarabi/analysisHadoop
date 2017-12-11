#mkdir inputFolder
for i in $(find `pwd` -name "*.gzip"); 
do 
	    mv "$i" "${i%.gzip}.gz"
	    #mv "${i%.gzip}.gz" inputFolder
done

#Move all Files into one Folder. 
#find 'pwd' -name '*.gz' | xargs -I files mv files outputFolder
