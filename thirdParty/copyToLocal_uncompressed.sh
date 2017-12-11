for i in $(find `pwd` -name "*.gz"); 
do 
	    mv "$i" "${i%.gz}.gzip"
done

#Move all Files into one Folder. 
#find 'pwd' -name '*.gz' | xargs -I files mv files outputFolder
