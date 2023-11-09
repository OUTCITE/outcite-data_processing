infolder=/home/outcite/openalex/openalex-snapshot/data/works/
script=/home/outcite/data_processing/code/index_openalex.py
updated_file=/home/outcite/data_processing/resources/updated_date
read updated_date < $updated_file
updated_date_int=$(date -d $updated_date +%s)

most_recent=$updated_date_int

for folder in ${infolder}*/; do
    foldername="${folder%/}";
    foldername="${foldername##*/}";
    folder_date="${foldername##updated_date=}"
    folder_date_int=$(date -d $folder_date +%s)
    if [ $updated_date_int -lt $folder_date_int ]; then
        echo Folder date $folder_date more recent than $updated_date, where last updated. Processing...
        python $script $folder upsert;
        if [ $most_recent -lt $folder_date_int ]; then
            most_recent=$folder_date_int;
            echo "$most_recent" > $updated_date_int;
        fi
    fi
done
