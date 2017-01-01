#!/usr/bin/env python

# Licensed under the GNU GENERAL PUBLIC LICENSE, Version 3, 29 June 2007 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://choosealicense.com/licenses/gpl-3.0/
#     or license.gpl included in the project
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.






import logging
import boto3
import os
import sys
import hashlib
import binascii
import argparse
import time
import threading
import Queue
import operator

parser = argparse.ArgumentParser(description="Upload files to S3")
parser.add_argument("-b", "--bucket-name", help="Bucket name to upload to", required=True)
parser.add_argument("-f", "--folder-to-upload", help="Folder to upload", required=True)
parser.add_argument("-c", "--config-file", help="Full path to the config file", default='s3upload.conf')
parser.add_argument("-l", "--log-file", help="Full path to the log file")
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")

logger = logging.getLogger(os.path.basename(__file__))


def upload(client_s3, path_s3, _path, bucket):

    try:
        client_s3.upload_file(_path, bucket, path_s3)
        logger.info('File: {} uploaded'.format(path_s3))
        return True
    except:
        logger.warning('Failed to upload file: {}'.format(path_s3), exc_info=True)


def _upload_part(upl_client, bct, body_part, s3_key, part_no, upload_id, queue):

    try:
        rsp = upl_client.upload_part(Bucket=bct,
                                    Body=body_part,
                                    Key=s3_key,
                                    PartNumber=part_no,
                                    UploadId=upload_id
                                    )
        logger.debug('Part {} of {} uploaded.'.format(part_no, s3_key))

        queue.put({'ETag': rsp['ETag'], 'PartNumber': part_no})

    except:
        logger.warning('Failed to upload a chunk of file: {}'.format(s3_key), exc_info=True)


def _process_threads(threads_):

    logger.debug('Threads collected: {}. Processing ...'.format(threads_))
    for th in threads_:
        th.start()
        logger.debug('Thread started: {}'.format(th))
    for th in threads_:
        th.join()
    logger.debug('Threads: {} joined'.format(len(threads_)))
    return []


def upload_multipart(client_s3, path_s3, _path, chunk_size, bucket, max_threads):

    mpu = None
    queue = Queue.Queue()
    threads = []

    try:
        # initialize multipart upload
        mpu = client_s3.create_multipart_upload(Bucket=bucket, Key=path_s3)
        logger.debug('Initialization of {} success with info: {}'.format(path_s3, mpu))
        part = 0
        part_info = {
            'Parts': [
            ]
        }
        logger.debug('Start uploading parts to: {}'.format(path_s3))

        with open(_path, 'r') as req:
            # uploading parts
            while True:
                body = req.read(chunk_size)
                if body:
                    part += 1
                    logger.debug('Creating thread for part no {} of: {}'.format(part, path_s3))
                    thread_ = threading.Thread(target=_upload_part, args=(client_s3, bucket, body, path_s3,
                                                                       part, mpu['UploadId'], queue, ))
                    threads.append(thread_)

                    if len(threads) == max_threads:
                        logger.debug('Max. threads number reached')
                        threads = _process_threads(threads)
                else:
                    break

            _process_threads(threads)

            # etag = {'ETag': rsp['ETag'], 'PartNumber': part_no}
            # The list of parts was not in ascending order. Parts must be ordered by part number.
            etags = []
            while not queue.empty():
                etags.append(queue.get())

            etags.sort(key=operator.itemgetter('PartNumber'))

            for etag in etags:
                part_info['Parts'].append(etag)

            logger.debug('Multipart upload {} finished. Start completing...'.format(path_s3))

        # complete the multipart upload
        # min chunk size is 5MB
        client_s3.complete_multipart_upload(Bucket=bucket,
                                              Key=path_s3,
                                              MultipartUpload=part_info,
                                              UploadId=mpu['UploadId']
                                              )
        logger.info('Multipart upload completed!')
        return True

    except:
        logger.warning('Failed to upload file: {}'.format(path_s3), exc_info=True)
        if mpu:
            logger.debug('Aborting the upload of {}...'.format(path_s3))
            client_s3.abort_multipart_upload(
                Bucket=bucket,
                Key=path_s3,
                UploadId=mpu['UploadId'])
            logger.info('Upload of {} aborted!'.format(path_s3))


def check_md5_checksum(client_s3, path_s3, _path, bucket, chunk_size):

    """

    :param client_s3:
    :param path_s3: path to object in s3 bucket
    :param _path: local file path
    :param bucket: s3 bucket name
    :param chunk_size: size of the chunk if file uploaded in chunks
    :return:
    """

    def get_md5(file_path, chunks):

        if chunks:
            md5string = ""
            block_count = 0

            with open(file_path, 'rb') as hlr:
                for block in iter(lambda: hlr.read(chunk_size), ""):
                    hsh = hashlib.md5()
                    hsh.update(block)
                    md5string = md5string + binascii.unhexlify(hsh.hexdigest())
                    block_count += 1

            hsh = hashlib.md5()
            hsh.update(md5string)
            return hsh.hexdigest() + "-" + str(block_count)
        else:
            return hashlib.md5(open(file_path, 'rb').read()).hexdigest()

    try:
        md5sum_s3 = client_s3.head_object(
            Bucket=bucket,
            Key=path_s3
        ).get('ETag')[1:-1]
        logger.debug('md5 of s3 object file: {} is: {}'.format(path_s3, md5sum_s3))
    except:
        logger.warning('Failed to take md5 of file: {}'.format(path_s3), exc_info=True)
        return False

    md5sum_file = get_md5(_path, chunk_size)
    logger.debug('md5 of local file: {} is: {}'.format(_path, md5sum_file))

    if md5sum_s3 == md5sum_file:
        logger.debug('md5 verified: {}'.format(path_s3))
        return True
    else:
        logger.warning('Upload file and S3 file md5 dont match: {} : {}.\n'
                       'File: {} will not be deleted'.format(md5sum_s3, md5sum_file, _path))


def delete(_path):

    try:
        os.remove(_path)
        logger.debug('Deleted file: {}'.format(_path))
    except:
        logger.error('Error deleting file: {}'.format(_path), exc_info=True)


def get_conf(conf_file):

    """
    custom config parser
    lines which start with # are omitted
    :param conf_file: a full path to the configuration file
    :return: config dict
    '"""

    cf = {}
    with open(conf_file) as hlr:
        for line in hlr:
            if not line.startswith('#'):
                split_line = line.split("=")
                cf[split_line[0].strip()] = split_line[1].strip()
    return cf


def main(conf, source_path, bucket_name):
    client = boto3.client('s3', aws_access_key_id=conf.get('access_key'),
                                       aws_secret_access_key=conf.get('secret_key'))

    num_retries = int(conf['num_retries'])
    max_threads = int(conf['max_threads'])

    delete_file = True if conf['delete_file'].lower() == 'true' else False

    # folder to upload from
    dest_folder_s3 = os.path.basename(source_path)
    logger.info('Source: {}, Destination --> bucket: {}, folder: {}'.format(source_path, bucket_name, dest_folder_s3))

    t1 = time.time()

    for root, dirs, files in os.walk(source_path):

        for filename in files:

            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, source_path)
            s3_key = os.path.join(dest_folder_s3, relative_path)

            file_size = os.path.getsize(local_path)
            logger.debug('File to upload: {} as s3 key: {} of size: {}'.format(local_path, s3_key, file_size))

            for _ in range(num_retries):

                bytes_per_chunk = int(conf['bytes_per_chunk'])

                if file_size > bytes_per_chunk:
                    logger.info('Upload in chunks of: {} size'.format(bytes_per_chunk))
                    uploaded = upload_multipart(client, s3_key, local_path, bytes_per_chunk, bucket_name, max_threads)
                else:
                    bytes_per_chunk = None
                    uploaded = upload(client, s3_key, local_path, bucket_name)

                if uploaded:
                    if check_md5_checksum(client, s3_key, local_path, bucket_name, bytes_per_chunk):
                        if delete_file:
                            delete(local_path)
                        break
                else:
                    logger.warning('Upload & checksum check of: {} failed'.format(s3_key))
            else:
                logger.error('File: {} failed to upload max. ({}) number of times'.format(s3_key, num_retries))

    t2 = time.time() - t1
    logger.info('Upload complete. Time taken to complete: %0.2fs' % t2)


if __name__ == '__main__':

    args = parser.parse_args()
    arg_dict = vars(args)

    folder_to_upload = arg_dict['folder_to_upload']
    bucket_name = arg_dict['bucket_name']

    cnf = get_conf(arg_dict['config_file'])

    if arg_dict['verbose']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.getLevelName(cnf.get('log_level', 'INFO')))
    if not arg_dict['log_file']:
        log_file = os.path.join(os.path.dirname(__file__), 'log',
                                time.strftime('%d%m%y%H%M', time.localtime()) + "_s3upload.log")
    else:
        log_file = arg_dict['log_file']
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    logger.debug('CLI args: {}'.format(arg_dict))
    logger.debug('Config key, value pairs: {}'.format(cnf))

    main(cnf, folder_to_upload, bucket_name)