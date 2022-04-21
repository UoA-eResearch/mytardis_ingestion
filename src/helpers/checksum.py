"""Helper functions to calculate checksums for MyTardis

MyTardis uses MD5 checksums and S3 eTag checksums in determining that
known-good copies of files are made. These scripts provide wrappers around
python hashlib and around OS system checksum calculators for ease of use.

Attributes:
    KB: A constant representing a kilobyte
    MB: A constant representing a megabyte
    GB: A constant representing a gigabyte
    TB: A constant representing a terrabyte
"""

import hashlib
import os
import subprocess

KB = 1024
MB = KB ** 2
GB = KB ** 3
TB = KB ** 4


def md5_python(file_path, blocksize):
    """Calculates the MD5 checksum of a file using python hashlib

    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.

    Args:
        file_path: The file path for creating checksum from
        blocksize: The size of the file 'chunk' streamed to hashlib

    Returns:
        The hex encoded MD5 checksum.
    """

    md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as infile:
            while True:
                chunk = infile.read(blocksize)
                if not chunk:
                    break
                md5.update(chunk)
        md5sum = md5.hexdigest()
    except Exception as err:
        raise err
    return md5sum


def md5_subprocess(file_path, md5sum_executable):
    """Calculates the MD5 checksum of a file using the system executable

    Calculates the MD5 checksum of a file, returns the hex digest as a
    string. Streams the file in chunks of 'blocksize' to prevent running
    out of memory when working with large files.

    Args:
        file_path: The file path for creating checksum from
        md5sum_executable: The system executable to calculate an MD5 sum

    Returns:
        The hex encoded MD5 checksum.
    """

    try:
        out = subprocess.check_output([md5sum_executable, file_path])
        md5sum = out.split()[0]
        if len(md5sum) != 32:
            raise ValueError(f"md5sum failed: {out}")
        return md5sum
    except Exception as error:
        raise error


def calculate_md5sum(
    file_path,
    blocksize=128,
    subprocess_size_threshold=10 * MB,
    md5sum_executable="/usr/bin/md5sum",  # NB: Linux specific file path
):
    """
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
    """
    if (
        os.path.getsize(file_path) > subprocess_size_threshold
        and os.path.exists(md5sum_executable)
        and os.access(md5sum_executable, os.X_OK)
    ):
        try:
            md5sum = md5_subprocess(file_path, md5sum_executable=md5sum_executable)
        except Exception as err:
            raise err
    else:
        try:
            md5sum = md5_python(file_path, blocksize=blocksize)
        except Exception as err:
            raise err
    try:
        md5sum = md5sum.decode()
    except (UnicodeDecodeError, AttributeError):
        pass
    except Exception as err:
        raise err
    return md5sum
