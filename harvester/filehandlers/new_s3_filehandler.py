'''Amazon s3 uploader scripts

Uploads a file list to s3 storage prior to linking back for myTardis ingestion.

Written by: Chris Seal
email: c.seal@auckland.ac.nz
'''

__author__ = 'Chris Seal <c.seal@auckland.ac.nz>'

from . import FileHandler
import boto3
import os
import sys
import hashlib
import logging
import subprocess
from ..helper import constants as CONST
from ..helper import helper as hlp
from pathlib import Path

logger = logging.getLogger(__name__)

class S3FileHandler(FileHandler):
    
