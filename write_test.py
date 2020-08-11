#!/usr/bin/python3
import os
import sys
import boto3
import botocore
from smart_open import open
import re
import hashlib
import codecs
import argparse
import json

class S3RSync:
  #Temporary file used in the S3 copy. Needed only while the concurrent S3 session bug exists.
  TEMP_FILENAME = '/tmp/s3_tmp'
  CHUNK_SIZE = 1073741824 #1G

  def __init__(self, src_keys, src_endpoint, dest_keys, dest_endpoint, chunk_size=CHUNK_SIZE, debug = 0, update = True):
    '''
      Cache s3 credentials for later use.
      
      src_keys:      S3 key ID and secret of the source S3 server.
      src_endpoint:  URI for the source S3 server
      dest_keys:     S3 key ID and secret for the destination S3 server
      dest_endpoint: URI for the destination S3 server
      self.chunk_size:    Read/write buffer size. Affects the S3 ETag, when more data size > chunk size.
    '''

    self.src_session = boto3.Session(
         aws_access_key_id=src_keys['access_key_id'],
         aws_secret_access_key=src_keys['secret_access_key']
    )
    self.src_connection = self.src_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=src_endpoint,
        config=None
    )
    self.src_endpoint = src_endpoint

    self.dest_session = boto3.Session(
         aws_access_key_id=dest_keys['access_key_id'],
         aws_secret_access_key=dest_keys['secret_access_key']
    )
    self.dest_connection = self.dest_session.client(
        's3',
        aws_session_token=None,
        region_name='us-east-1',
        use_ssl=True,
        endpoint_url=dest_endpoint,
        config=None
    )
    self.dest_endpoint = dest_endpoint
    
    self.chunk_size = chunk_size
    self.debug = debug
    self.update = update

  def read_in_chunks(self, file_object):
    '''
      Iterator to read a file chunk by chunk.
    
      file_object: file opened by caller
    '''
    while True:
      data = file_object.read(self.chunk_size)
      if not data:
        break
      yield data

  def etag(self, md5_array):
    ''' 
      Calculate objects ETag from array of chunk's MD5 sums
    
      md5_array: md5 hash of each buffer read
    '''
    if len(md5_array) < 1:
      return '"{}"'.format(hashlib.md5().hexdigest())

    if len(md5_array) == 1:
      return '"{}"'.format(md5_array[0].hexdigest())

    digests = b''.join(m.digest() for m in md5_array)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5_array))

  def s3copys3(self, src_bucket, dest_bucket, key, size, disable_multipart = False):
    ''' 
      S3 copy from source S3 object to destination s3 object ( renamed as src_bucket/original_object_name )
      
      src_bucket:  Source S3 bucket for the object to be copied
      dest_bucket: destination S3 bucket to copy the object to
      key:         Object name
      size:        length of object, in bytes. Used to determine if we write in chunks, or not (which affects the ETag created)
    '''
    source_s3_uri = 's3://{}/{}'.format(src_bucket, key)
    dest_s3_uri = 's3://{}/{}/{}'.format(dest_bucket, src_bucket, key)

    multipart = size > self.chunk_size
    if disable_multipart: multipart = False #Might have an original ETag, that wasn't multipart, but bigger than than the chunk_size.

    '''
    #Bug in S3 library corrupts read buffer when more than one s3 stream is open. 

    with open(source_s3_uri, 'rb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}}, ignore_ext=True) as s3_source:
      with open(dest_s3_uri, 'wb', transport_params={'session': self.dest_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.dest_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(s3_source):
           s3_destination.write(chunk)
    '''
    
    #Temporary code, While concurrent S3 session bug exists
    
    #Read from source S3; Write to temporary file
    with open(source_s3_uri, 'rb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}}, ignore_ext=True) as s3_source:
      with open(self.TEMP_FILENAME, 'wb') as fout:
        for chunk in self.read_in_chunks(s3_source):
           fout.write(chunk)
           
    #Read from temporary file; Write to destination S3.
    with open(self.TEMP_FILENAME, 'rb') as fin:
      with open(dest_s3_uri, 'wb', transport_params={'session': self.dest_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.dest_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
           s3_destination.write(chunk)
           
    os.remove(self.TEMP_FILENAME)

  def bucket_ls(self, s3, bucket, prefix="", suffix=""):
    '''
    Generate objects in an S3 bucket. Derived from AlexWLChan 2019

    :param s3: authenticated client session.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with this prefix (optional).
    :param suffix: Only fetch objects whose keys end with this suffix (optional).
    '''
    paginator = s3.get_paginator("list_objects") # should be ("list_objects_v2"), but only getting first page with this

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
      prefixes = (prefix, )
    else:
      prefixes = prefix

    for key_prefix in prefixes:
      kwargs["Prefix"] = key_prefix

      for page in paginator.paginate(**kwargs):
        try:
          contents = page["Contents"]
        except KeyError:
          break

        for obj in contents:
          key = obj["Key"]
          if key.endswith(suffix):
            yield obj

  def s3destls(self, bucket, prefix ):
    for r in self.bucket_ls(s3 = self.dest_connection, bucket = bucket, prefix = '{}/'.format(prefix)):
      print(r['Key'], ' ', r['Size'], ' ', r['LastModified'], ' ', r['ETag'])
  
  def copy_file_mytardis(self, filename, bucket, prefix = None):
    #Test of write to source S3
    #Read from file; Write to src S3.
    object_name = os.path.basename(filename) if prefix is None else '{}/{}'.format(prefix, os.path.basename(filename))
    source_s3_uri = 's3://{}/{}'.format(bucket, object_name)
    multipart = False

    with open(filename, 'rb') as fin:
      with open(source_s3_uri, 'wb', transport_params={'session': self.src_session,  'buffer_size': self.chunk_size, 'resource_kwargs': { 'endpoint_url': self.src_endpoint}, 'multipart_upload': multipart}, ignore_ext=True) as s3_destination:
        for chunk in self.read_in_chunks(fin):
           s3_destination.write(chunk)
    
  def rsync(self, src_bucket, dest_bucket):
    '''
      Copy all objects in the source S3 bucket to the destination S3 bucket
      Prefixing the destination key with the source bucket name.
    
      src_bucket: bucket name
      dest_bucket: bucket name
    '''
    #Preseed the destination bucket object keys dictionary with the Object ETags.
    dest_keys = {}
    for r in self.bucket_ls(s3=self.dest_connection, bucket=dest_bucket, prefix='{}/'.format(src_bucket)):
      key = re.sub(r"^{}/".format(src_bucket), '', r['Key'])
      dest_keys[key] = r['ETag']
      if self.debug >= 3: print('DEST: ', r['Key'], ' ', r['ETag'], ' ', r['Size']) 

    #Get the object headers from the source S3, and see if these need to be copied.
    for r in self.bucket_ls(s3=self.src_connection, bucket=src_bucket):
      if r['Key'] in dest_keys:
        if dest_keys[r['Key']] == r['ETag']:
          if self.debug >= 2: print('Exists:  ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size'])
        else: 
          #Have the object, but the ETag differs, so we need to copy, then deleted the last version
          if self.debug >= 1: print('Mismatched: ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size'], dest_keys[r['Key']])
          if self.update: 
            self.s3copys3(src_bucket=src_bucket, dest_bucket=dest_bucket, key=r['Key'], size=r['Size'], disable_multipart = ('-' not in r['ETag']) )
            #Delete previous object, if store has versioning on.
        dest_keys[r['Key']] = None  #So we know we have this one in the source.
      else:
        if self.debug >= 1: print('Copying: ', src_bucket, "/", r['Key'], ' ', r['ETag'], ' ', r['Size']) 
        if self.update: self.s3copys3(src_bucket=src_bucket, dest_bucket=dest_bucket, key=r['Key'], size=r['Size'], disable_multipart = ('-' not in r['ETag']))

    for r in dest_keys:
      if dest_keys[r] is not None:
        if self.debug >= 2: print('DELETED: ', r)
        #Might want to age out the deleted ones.

def parse_args():
  parser = argparse.ArgumentParser(description='rsync from source s3 bucket to dest s3 bucket')
  parser.add_argument('-?', action='help', default=argparse.SUPPRESS, help=argparse._('show this help message and exit'))
  parser.add_argument('-d', '--debug', dest='debug_lvl', default=0,  help="0: Off, 1: Copy/Mismatch messages, 2: Exists messages, 3: Dest ls", type=int)
  parser.add_argument('-n', '--no_rsync', dest='no_rsync', action='store_true', help='Use with Debugging. Default is to perform s3 rsync')
  parser.add_argument('-c', '--conf', dest='conf_file', default=None, help='Specify JSON conf file for source and destination')
  parser.add_argument('-a', '--auth', dest='auth_file', default=None, help='Specify JSON auth file containing s3 keys')
  parser.add_argument('--ls', dest='dest_ls', action='store_true', help='directory listing of S3 backup')
  parser.add_argument('--cp', dest='copy_file', default=None,  help='copy a file to myTardis S3')
  args = parser.parse_args()
  '''
  if args.conf_file is None or args.auth_file is None:
    parser.print_help(sys.stderr)
    sys.exit(1)
  ''' 
  return args

def json_load(filename):
  try:
    with open( filename ) as f:
      return json.load(f)
  except Exception as e:
    print( "json_load({}): ".format(filename), e )
    sys.exit(1)

def main():
  args = parse_args()
  if args.auth_file is None:
    auth = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/test_auth.json')
  else:
    auth = json_load(args.auth_file)
    
  if args.conf_file is None:
    conf = json_load(os.path.dirname(os.path.realpath(__file__)) + '/conf/test_conf.json')
  else:
    conf = json_load(args.conf_file)

  if 'src_s3_keys' not in auth: sys.exit("src_s3_keys not defined in auth file")
  if 'src_endpoint' not in auth: sys.exit("src_endpoint not defined in auth file")
  if 'dest_s3_keys' not in auth: sys.exit("dest_s3_keys not defined in auth file")
  if 'dest_endpoint' not in auth: sys.exit("dest_endpoint not defined in auth file")

  if 'src_buckets' not in conf: sys.exit("src_buckets not defined in conf file")
  if 'dest_bucket' not in conf: sys.exit("dest_bucket not defined in conf file")

  if 'chunk_size' not in conf or type(conf['chunk_size']): conf['chunk_size'] = S3RSync.CHUNK_SIZE 

  s3rsync = S3RSync(src_keys=auth['src_s3_keys'], src_endpoint=auth['src_endpoint'], dest_keys=auth['dest_s3_keys'], dest_endpoint=auth['dest_endpoint'], debug=args.debug_lvl, chunk_size = conf['chunk_size'], update=(not args.no_rsync))

  '''
  if args.dest_ls:
    for bucket in conf['src_buckets']:
      s3rsync.s3destls( bucket = conf['dest_bucket'], prefix = bucket )
  elif args.copy_file is not None:
    s3rsync.copy_file_mytardis(filename = args.copy_file, bucket = 'mytardis', prefix='Write_Test')
  else:
    for bucket in conf['src_buckets']:
      s3rsync.rsync(src_bucket=bucket, dest_bucket=conf['dest_bucket'])
  '''
  filename = os.path.dirname(os.path.realpath(__file__)) + '/data/snark.txt'
  s3rsync.copy_file_mytardis(filename = filename, bucket = 'mytardis', prefix='Write_Test')
  
  
if __name__ == "__main__":
  main()
