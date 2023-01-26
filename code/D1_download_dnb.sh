
download_script=/home/outcite/data_processing/code/M_download_files_with_ext.py
dnb_folder=/home/outcite/dnb/

for file in ${dnb_folder}*.mrc.xml; do
    mv $file ${dnb_folder}old/;
done

python $download_script --ext '.mrc.xml.gz' https://data.dnb.de/DNB/ ${dnb_folder}

for file in ${dnb_folder}*.mrc.xml.gz; do
    echo decompressing ${file}...
    gzip -d $file;
done

echo Done downloading latest version of DNB into ${dnb_folder}.
