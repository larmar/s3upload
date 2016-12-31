## Installation:

* Install Amazon Web Services (AWS) SDK for Python and ArgumentParser
```
pip install boto3
pip install argparse
```
* Rename s3upload.conf.template to s3upload.conf  


## What it does:

* uploads files from a folder into the bucket
* mirrors the entire folder structure in the bucket along with the files uploaded
* when a file is larger than bytes_per_chunk, splits the files into chunks and uploads simultaneously these chunks,
the number if threads is determined by max_threads
* when a file is uploaded, it is deleted unless delete_file is set to false
* once a file is uploaded, the script checks md5 sumcheck
* if upload fails, the script retries to upload the file, determined by num_retries

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