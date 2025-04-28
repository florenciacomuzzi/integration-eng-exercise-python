"""Utilities for working with AWS S3."""
import logging
import requests

import boto3
from boto3.s3.transfer import TransferConfig


logger = logging.getLogger(__name__)


class S3Helper(object):
    """Helper class for interacting with S3."""

    default_config = TransferConfig()

    def __init__(self, bucket_name, region_code, profile=None, config=None):
        """Connect to S3 and perform basic config.

        Args:
            bucket_name (str):          Name of bucket to use
            profile (str or None):      Name of profile in local config
                                        (use None for IAM)
            config (boto3.s3.transfer.TransferConfig or None):
                    Transfer configuration to override the default
        """
        self._connect(profile=profile)
        self.bucket_name = bucket_name
        self.bucket = self.resource.Bucket(bucket_name)
        self.region_code = region_code
        self.default_config.region_name = region_code
        self.config = config if config else self.default_config

    def _connect(self, profile=None):
        if profile:
            session = boto3.session.Session(profile_name=profile)
            self.resource = session.resource('s3')
            self.client = session.client('s3')
        else:
            self.resource = boto3.resource('s3')
            self.client = boto3.client('s3')

    def _clean_s3_path(self, path):
        """Clean extraneous slashes from an S3 path (key or prefix).

        Care must be taken to handle slashes properly in an S3 path.
          - A double slash is interpreted by S3 as a folder whose name is an
            empty string.
          - A leading or trailing slash in what's intended to be a relative
            path can be interpreted as an absolute path in some circumstances.
        For these reasons, both leading and trailing slashes are removed, and
        double slashes are replaced with a single slash.

        Args:
            path (str):     S3 path to a key or prefix

        Returns:
            str:            Cleaned path
        """
        path = path.strip('/')
        if '//' in path:
            logger.warning(
                'Double slash found in S3 path {}, replacing with single slash'
                .format(path)
            )
            path = path.replace('//', '/')
        return path

    def download_key_with_presigned_url(self, key, dest_path, expiration=3600):
        """Download a single S3 key (file) using direct HTTP access without AWS authentication.
        Note: This will only work for public S3 buckets or objects that have been made publicly accessible.

        Args:
            key (str):          S3 key (everything except the bucket)
            dest_path (str):    Local destination path
            expiration (int):   Not used, kept for backward compatibility
        """
        key = self._clean_s3_path(key)

        # Construct the public S3 URL
        url = f"https://{self.bucket_name}.s3.amazonaws.com/{key}"

        # Download using requests
        response = requests.get(url)
        response.raise_for_status()

        # Save to file
        with open(dest_path, 'wb') as f:
            f.write(response.content)
