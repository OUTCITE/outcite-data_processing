#The most recent snapshot URl needs to be manually fetched by filling out the form on their website
URL="https://www.google.com/url?q=https://unpaywall-data-snapshots.s3.us-west-2.amazonaws.com/unpaywall_snapshot_2022-03-09T083001.jsonl.gz&sa=D&source=editors&ust=1680512742274304&usg=AOvVaw3OwEYotSS_60lsBAkX4sCg";

wget $URL -O /home/outcite/data_processing/resources/unpaywall.jsonl.gz

gzip -d /home/outcite/data_processing/resources/unpaywall.jsonl.gz

python /home/outcite/data_processing/code/load_unpaywall.py /home/outcite/data_processing/resources/unpaywall.jsonl /home/outcite/data_linking/resources/unpaywall.db
