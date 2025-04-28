"""Utilities for working with AWS S3."""
import io
import json
import logging
import os

import boto3
from urllib.parse import urlparse

from botocore import exceptions as boto_exceptions
from boto3.s3.transfer import TransferConfig


logger = logging.getLogger(__name__)


class S3Helper(object):
    """Helper class for interacting with S3."""

    default_config = TransferConfig()

    def __init__(self, bucket_name, profile=None, config=None):
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

    def download_key(self, key, dest_path):
        """Download a single S3 key (file) to the local file system.

        Args:
            key (str):          S3 key (everything except the bucket)
            dest_path (str):    Local destination path
        """
        key = self._clean_s3_path(key)
        self.bucket.download_file(key, dest_path, Config=self.config)

    def upload_key(self, key, src_path):
        """Upload a single S3 key (file) from the local file system.

        Args:
            key (str):          S3 key (everything except the bucket)
            src_path (str):     Local source path
        """
        key = self._clean_s3_path(key)
        self.bucket.upload_file(src_path, key, Config=self.config)

    def upload_json(self, key, data):
        """Upload a data set as json.

        Args:
            key (str):     S3 key (everything except the bucket)
            data ([]):     JSON Data set
        """
        key = self._clean_s3_path(key)
        obj = self.resource.Object(self.bucket_name, key)
        obj.put(Body=json.dumps(data))

    def upload_byte_stream(self, S3_key, byte_stream):
        """Upload a schema which is already encoded as json string by json.dumps()
        Note the S3_key does NOT support full s3 url, e.g., "s3://..."
        Args:
            S3_key (str):     S3 key (everything except the bucket)
            byte_stream (str/bytes):     Data in string or bytes representation
        """
        self.resource.Object(self.bucket_name, S3_key).put(Body=byte_stream)
        logger.info("uploaded byte stream to S3 bucket {bucket} with target key {S3_key}"
                    .format(bucket=self.bucket_name, S3_key=S3_key))

    def download_prefix(self, prefix, dest_dir):
        """Download all keys (files) within a prefix (folder) in the S3 bucket.

        Preserves S3 folder structure (excluding prefix). For example:
            Source key:     test/folder_1/folder_2/sample.txt
            Prefix:         test/folder_1/
            Destination:    /tmp/
            Result:         /tmp/folder_2/sample.txt

        Args:
            prefix (str):       Prefix (folder) to download the contents of
            dest_dir (str):     Local destination directory
        """
        prefix = self._clean_s3_path(prefix)
        matching_objects = self.bucket.objects.filter(Prefix=prefix)
        for obj in matching_objects:
            key = obj.key
            if not key.endswith('/'):
                obj_rel_path = key.replace(prefix, '').lstrip('/')
                obj_dest_path = os.path.join(dest_dir, obj_rel_path)
                os.makedirs(os.path.dirname(obj_dest_path), exist_ok=True)
                self.download_key(key, obj_dest_path)

    def list_keys(self, prefix=None, list_objects=False):
        """List the keys in a bucket or under a prefix.

        Args:
            prefix (str):       Prefix (folder) to list the contents of
            list_objects (bool) : Optionally, yield objects.
                    Default is False so keys are yielded.

        Yields:
            str:        Key name
        """
        if prefix:
            prefix = self._clean_s3_path(prefix)
            objects = self.bucket.objects.filter(Prefix=prefix)
        else:
            objects = self.bucket.objects.all()
        if list_objects:
            for obj in objects:
                yield obj
        else:
            for obj in objects:
                yield obj.key

    def read_byte_stream(self, s3key):
        try:
            return self.resource.Object(self.bucket_name, s3key).get()['Body'].read()
        except Exception:
            logger.info(
                'Failed to retrieve data from bucket {bucket} with target key {S3_key}'
                .format(bucket=self.bucket_name, S3_key=s3key))
            return None

    def get_size(self, prefix=None):
        """Returns total bytes of contents in a bucket or under a prefix.

        :param prefix (str) : Optional. Prefix (folder) to get size of.
        :return: total bytes of content
        """
        total_bytes = 0
        for obj in self.list_keys(prefix, list_objects=True):
            total_bytes += obj.size
        return total_bytes

    def delete_all_keys_with_prefix(self, prefix):
        """Delete all keys under a given prefix.
        Args:
            prefix (str): Prefix (folder) in S3
        """
        keys = self.list_keys(prefix=prefix)
        if keys:
            logger.warning("Deleting all keys under prefix {}".format(prefix))
            self.delete_keys(keys)

    def rename_prefix(self, prefix, new_prefix):
        """Rename S3 prefix. This method is useful for Spark dumps using
        partitionBy which names S3 prefix as '.../{partition-field}={value}'
        which may not be the desirable naming convention.

        :param prefix (str): Original S3 prefix to rename.
        :param new_prefix (str): New S3 prefix.
        """
        prefix = prefix[:-1] if prefix.endswith('/') else prefix
        new_prefix = new_prefix[:-1] \
            if new_prefix.endswith('/') else new_prefix
        for key in self.list_keys(prefix):
            filename = key.split('/')[-1]
            new_key = '{new_prefix}/{filename}'.format(new_prefix=new_prefix,
                                                       filename=filename)
            source = '{bucket}/{key}'.format(bucket=self.bucket_name, key=key)
            self.resource.Object(self.bucket_name, new_key).copy_from(
                CopySource=source)
            self.resource.Object(self.bucket_name, key).delete()

    def list_subfolders_under_folder(self, s3folder):
        """
        list subfolders under given s3folder/s3prefix. Note in corner cases when the target s3folder is a leaf folder,
        this function will return its underlying s3keys and treat them as 'subfolders'.

        Parameters
        ----------
        s3folder : str
                   target s3prefix/folder


        Note
        --------
        to make the return consistent, if the target folder is a leaf folder and s3keys are returned,
        then they will be appended by folder symbol '/'.

        Returns
        --------
        iterator
         an iterator of either keys or subfolders, note iterator can be empty.
        """
        result = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=s3folder, Delimiter='/')
        folder_entries = result.get('CommonPrefixes')
        if folder_entries:
            for entry in folder_entries:
                yield entry['Prefix']
        else:
            key_entries = result.get('Contents')
            if key_entries:
                for key_entry in key_entries:
                    s3key = key_entry['Key']
                    if s3key != s3folder:  # rule out the s3folder itself
                        yield s3key + '/'


    def get_all_s3_objects(self, prefix):
        # work around AWS's limit of listing up to 1000 objects in S3 prefix
        continuation_token = None
        while True:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, MaxKeys=1000,
                                                   ContinuationToken=continuation_token) if continuation_token \
                else self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, MaxKeys=1000)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')


class S3Url(object):
    """parse s3 url into bucket and key"""

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()
