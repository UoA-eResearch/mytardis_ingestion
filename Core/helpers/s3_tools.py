import logging
import boto3
from botocore.exceptions import ClientError
import argparse

region = 'us-east-1'
parser = argparse.ArgumentParser()
parser.add_argument('endpoint_url',
                    help="The URL to the s3 endpoint")
parser.add_argument('bucket',
                    nargs='*',
                    help='Space delimited bucket names')
parser.add_argument('--create',
                    '-c',
                    help="Create the bucket if it doesn't exist",
                    action='store_true')
parser.add_argument('--files',
                    '-f',
                    help="Show the files in the bucket",
                    action="store_true")
args = parser.parse_args()


def create_bucket(endpoint_url,
                  bucket_name):
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=endpoint_url,
                                 region_name=region)
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as err:
        logging.error(err)
        return False
    except Exception as err:
        logging.error(err)
        raise
    return True


def list_buckets(endpoint_url):
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=endpoint_url,
                                 region_name=region)
        response = s3_client.list_buckets()
    except ClientError as err:
        logging.error(err)
        return []
    except Exception as err:
        logging.error(err)
        raise
    buckets = response['Buckets']
    bucket_list = []
    for bucket in buckets:
        bucket_list.append(bucket['Name'])
    return bucket_list


def list_files(endpoint_url,
               bucket_name):
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=endpoint_url,
                                 region_name=region)
        response = s3_client.list_objects_v2(Bucket=bucket)
    except ClientError as err:
        logging.error(err)
        return []
    except Exception as err:
        logging.error(err)
        raise
    file_list = []
    for obj in response['Contents']:
        file_list.append(obj['Key'])
    return file_list

if __name__ == '__main__':
    if args.bucket == []:
        print(list_buckets(args.endpoint_url))
    elif args.create:
        for bucket in args.bucket:
            created = create_bucket(args.endpoint_url,
                                    bucket)
            print(created)
    if args.files:
        for bucket in args.bucket:
            print(bucket)
            for fname in list_files(args.endpoint_url,
                                    bucket):
                print(fname)
                
