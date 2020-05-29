# checksum.py
#
# Functions to calculate checksums
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 29 May 2020
#

from . import constants as CONST
import hashlib
import subprocess


def md5_python(file_path,
               blocksize=None):
    '''
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    #
    ======================
    Inputs
    ======================
    file_path: The file path for creating checksum from
    blocksize: The size of the file 'chunk' streamed to hashlib
    #
    ======================
    Returns
    ======================
    md5sum: The hex encoded MD5 checksum.
    '''

    if not blocksize:
        blocksize = 128

    md5 = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(blocksize)
                if not chunk:
                    break
                md5.update(chunk)
        md5sum = md5.hexdigest()
    except Exception as err:
        raise
    return md5sum


def md5_subprocess(file_path,
                   md5sum_executable='/usr/bin/md5sum'):
    '''
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    #
    ======================
    Inputs
    ======================
    file_path: The file path for creating checksum from
    md5sum_executable: The system executable to calculate an MD5 sum
    #
    ======================
    Returns
    ======================
    md5sum: The hex encoded MD5 checksum.
    '''

    try:
        out = subprocess.check_output([md5sum_executable, file_path])
        md5sum = out.split()[0]
        if len(md5sum) == 32:
            return md5sum
        else:
            raise ValueError('md5sum failed: %s', out)
    except Exception as err:
        raise


def calculate_md5sum(file_path,
                     blocksize=None,
                     subprocess_size_threshold=10*CONST.MB,
                     md5sum_executable='/usr/bin/md5sum'):
    '''
    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.
    #
    If the file size is greater than subprocess_size_threshold and the
    md5sum tool exists, spawn a subprocess and use 'md5sum', otherwise
    use the native Python md5 method (~ 3x slower).
    #
    ======================
    Inputs
    ======================
    file_path: The file path for creating checksum from
    blocksize: The size of the file 'chunk' streamed to hashlib
    subprocess_size_threshold: Use the md5sum tool via a subprocess
                               for files larger than this. Otherwise
                               use Python native method. Default
                               10MB.
    ======================
    Returns
    ======================
    md5sum: The hex encoded MD5 checksum.
    '''
    if os.path.getsize(file_path) > subprocess_size_threshold and \
       os.path.exists(md5sum_executable) and \
       os.access(md5sum_executable, os.X_OK):
        try:
            md5sum = md5_subprocess(file_path,
                                    md5sum_executable=md5sum_executable)
        except Exception as err:
            raise
    else:
        try:
            md5sum = md5_python(file_path,
                                blocksize=blocksize)
        except Exception as err:
            raise
    try:
        md5sum = md5sum.decode()
    except(UnicodeDecodeError, AttributeError):
        pass
    except Exception as err:
        raise
    return md5sum


def calculate_etag(file_path,
                   blocksize):
    '''
    Calculates the S3 etag of a file. Since etags are calculated
    from md5 sums, chinks of file are streamed in order to prevent
    out of memory errors on large files.
    #
    ======================
    Inputs
    ======================
    file_path: The file path for creating checksum from
    blocksize: The size of the file 'chunk' streamed to hashlib
    #
    ======================
    Returns
    ======================
    etag: The Amazon s3 etag checksum of the file
    '''

    md5s = []
    try:
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(blocksize)
                if not chunk:
                    break
                md5s.append(hashlib.md5(chunk))
    except Exception as err:
        raise
    try:
        if len(md5s) > 1:
            digests = b"".join(m.digest() for m in md5s)
            new_md5 = hashlib.md5(digests)
            etag = new_md5.hexdigest() + '-' + str(len(md5s))
        elif len(md5s) == 1:
            etag = md5s[0].hexdigest()
        else:
            etag = '""'
    except Exception as err:
        raise
    return etag
