
openalex_folder=/home/outcite/openalex/openalex-snapshot/

aws s3 sync --delete "s3://openalex" ${openalex_folder} --no-sign-request
