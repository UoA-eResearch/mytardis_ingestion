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
from ..helper import constants as CONST

logger = logging.getLogger(__name__)
