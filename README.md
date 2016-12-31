## Installation:

* Install Amazon Web Services (AWS) SDK for Python and ArgumentParser
```
pip install boto3
pip install argparse
```
* Rename s3upload.conf.template to s3upload.conf


## What it does:

* uploads files from a folder into a S3 AWS bucket
* mirrors the entire folder structure in the bucket along with the files uploaded
* when a file is larger than __bytes_per_chunk__, splits the files into chunks and uploads simultaneously these chunks,
the number of threads spawn is determined by __max_threads__
* when a file is uploaded, it is deleted unless __delete_file__ is set to false
* once a file is uploaded, the script checks md5 sumcheck
* if upload fails, the script retries to upload, determined by __num_retries__

## How it works:

__Mandatory options:__
* -b or --bucket-name
* -f or --folder-to-upload

Example:
```
python s3upload.py -b logs -f /var/log
```

__Optional options:__
* -v or --verbose
* -c or --config-file
* -l or --log-file

Example:
```
python s3upload.py --bucket-name logs -f /var/log -v -l upload.log
```