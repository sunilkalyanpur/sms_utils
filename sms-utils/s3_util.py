"""
    Utility to copy file from ec2 instance to AWS s3

    Usage:
        file_copy_to_s3_util.py -k <aws_key> -s <aws_secret_key> -b <s3_bucket> [-l <s3_folder>] [-f <input_file>] [-d <source_dir>]

    Options:
        -k --aws_key <k>            aws key
        -s --aws_secret_key <s>     aws secret key
        -b --s3_bucket <b>          aws s3 bucket
        -l --s3_folder <l>          aws s3 folder under bucket
        -f --input_file <f>         input file to copy to s3
        -d --source_dir <d>         source directory

    s3 doc:
"""

import logging
import os

from boto.s3.connection import Location
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from docopt import docopt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The largest object that can be uploaded in a single PUT is 5 gigabytes.
# max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
# size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000


def traverse_dir(source_dir):
    """Traverse through directory to get list of files
        Note: This function won't traverse recursively
    """

    upload_filenames = []
    for (sourceDir, dirname, filenames) in os.walk(source_dir):
        upload_filenames.extend(filenames)
    return upload_filenames


def percent_cb(complete, total):
    """callback function that will be called to report
            progress on the upload."""
    percent_complete = (complete * 100 / total) if total !=0 else 0
    logger.info("Uploaded {} %".format(percent_complete))


def copy_to_s3(aws_key, aws_secret_key, s3_bucket, source_dir, s3_folder=None, input_file=None, location=Location.USWest2):
    """
    Copy file to s3
    """
    if not input_file and not source_dir:
        raise Exception("Either filename or source directory required")
    try:
        files_to_upload = []
        conn = S3Connection(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret_key)

        #  Attempts to get a bucket from S3. Returns None if bucket doesn't exist
        target_bucket = conn.lookup(bucket_name=s3_bucket)

        if target_bucket is None:
            # Creates a new located bucket
            target_bucket = conn.create_bucket(s3_bucket, location=location)

        if not input_file:
            files_to_upload = traverse_dir(source_dir=source_dir)
        else:
            files_to_upload.append(input_file)

        for fname in files_to_upload:
            source_file = os.path.join(source_dir + '/' + fname)
            key_name = "/" + s3_folder + '/' + fname if s3_folder else fname

            # Return the size of a file
            filesize = os.path.getsize(source_file)
            if filesize > MAX_SIZE:
                mp = target_bucket.initiate_multipart_upload(key_name=key_name)
                fp = open(source_file, 'rb')
                fp_num = 0
                while fp.tell() < filesize:
                    fp_num += 1
                    logger.debug("uploading part %i" % fp_num)
                    mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

                mp.complete_upload()
            else:
                logger.debug("single-part upload")
                k = Key(target_bucket)
                k.key = key_name
                k.set_contents_from_filename(source_file, cb=percent_cb, num_cb=10)
        return True
    except Exception as error:
        raise error


def main(opt):
    aws_key, aws_secret_key, s3_bucket, s3_folder, input_file, source_dir = opt['--aws_key'], opt['--aws_secret_key'], opt['--s3_bucket'], opt[
        '--s3_folder'], opt['--input_file'], opt['--source_dir']
    copy_to_s3(aws_key=aws_key, aws_secret_key=aws_secret_key, s3_bucket=s3_bucket, s3_folder=s3_folder, input_file=input_file, source_dir=source_dir)


if __name__ == '__main__':
    options = docopt(__doc__)
    main(options)
