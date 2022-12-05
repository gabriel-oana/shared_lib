import re
import logging
import traceback
from typing import Any, Optional, List
import boto3
from botocore.exceptions import ClientError


class S3:

    def __init__(self, region: str, client=None, resource=None, logger: Optional = None):
        self._region = region
        self._client = client if client else boto3.client('s3', region_name=region)
        self._resource = resource if resource else boto3.resource('s3', region_name=region)
        self._logger = logger if logger else logging

    def get_object(self, bucket_name: str, object_name: str, byte_range: str = None) -> Any:
        """
        Returns the contents of an object in a bucket.
        :param bucket_name: name of bucket
        :param object_name: name of object
        :param byte_range: bytes=0-9
        :return: Any
        """
        try:
            if byte_range:
                body = self._client.get_object(Bucket=bucket_name, Key=object_name, Range=byte_range)["Body"].read().decode()
            else:
                body = self._client.get_object(Bucket=bucket_name, Key=object_name)["Body"].read().decode()
            return body

        except ClientError as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    def put_object(self, bucket_name: str, object_name: str, body: Any) -> None:
        """
        Uploads the content of an object to S3.
        :param bucket_name: name of bucket
        :param object_name: name of object
        :param body: object body to be uploaded (the file will automatically be converted into byte form)
        :return: None
        """

        try:
            self._client.put_object(Bucket=bucket_name, Key=object_name, Body=str(body).encode('utf-8'))
        except ClientError as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    def delete_object(self, bucket_name: str, object_name: str) -> None:
        """
        Delete the object from S3
        :param bucket_name: name of bucket
        :param object_name: name of object
        """

        try:
            self._client.delete_object(Bucket=bucket_name, Key=object_name)
        except ClientError as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    @staticmethod
    def __convert_int_to_str_format(current_value: int, desired_value: str) -> float:
        """
        Converts the int from B to any desired value
        :param current_value:
        :param desired_value:
        :return:
        """
        valid_desired_values = ['B', 'KB', 'MB', 'GB', 'TB']
        if desired_value.upper() not in valid_desired_values:
            raise ValueError(f"Choose a valid desired value: {valid_desired_values}")

        lookup_table = {
            "B": 1,
            "KB": 1000,
            "MB": 1e6,
            "GB": 1e9,
            "TB": 1e12
        }
        return round(current_value / lookup_table[desired_value.upper()], 3)

    def get_object_size(self, bucket_name: str, object_name: str, str_format: str = 'MB') -> float:
        """
        Returns the size of a single object in the string format.
        :param bucket_name: name of bucket
        :param object_name: name of object
        :param str_format: B, KB, MB, GB, TB
        """
        try:
            obj = self._client.head_object(Bucket=bucket_name, Key=object_name)
            length = obj["ContentLength"]
            return self.__convert_int_to_str_format(length, str_format)

        except ClientError as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    def get_objects_size(self, bucket_name: str, prefixes: List[str], str_format: str = 'MB') -> float:
        """
        Returns the sum of the sizes of several files.
        :param bucket_name: name of bucket
        :param prefixes: list of object key prefixes
        :param str_format: B, KB, MB, GB, TB
        """
        total_size = 0
        for item in prefixes:
            paginator = self._client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=item)

            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        total_size += obj["Size"]
                else:
                    raise RuntimeError(f"No files found at {bucket_name}/{item}")

        size = self.__convert_int_to_str_format(total_size, desired_value=str_format)
        return size

    def get_matching_objects(self, bucket_name: str, prefix: str = '', suffix: str = '', regex_match: str = None,
                             strict: bool = False) -> List[dict]:
        """
        Gets a list of keys that match objects
        :param bucket_name: name of bucket
        :param prefix: prefix to match
        :param suffix: suffix to match (eg: .csv)
        :param regex_match: any regex string to match
        :param strict: raise if there are no files with selected patterns
        :return: list of dicts of s3 keys
        """
        matches = []
        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if "Contents" in page:
                for content in page["Contents"]:
                    key = content["Key"]

                    # Match suffix and prefix
                    if key.endswith(suffix) and key.startswith(prefix):
                        response = {
                            "bucket": bucket_name,
                            "key": key,
                            "file_name": key.split('/')[-1]
                        }

                        # Match regex
                        if regex_match:
                            if re.match(r'^' + regex_match + '$', key.split('/')[-1]):
                                matches.append(response)
                        else:
                            matches.append(response)

                # Add number of matched values to each dictionary inside the list
                items_length = len(matches)
                for item in matches:
                    item.update({"total_objects_matches": items_length})

        if len(matches) == 0 and strict:
            raise RuntimeError(
                f"No files were found in {bucket_name} with suffix {suffix}, prefix {prefix} and regex {regex_match}"
            )

        return matches

    def transfer_object(self, source_bucket: str, source_object: str, target_bucket: str, target_object: str) -> None:
        """
        Transfers an object from one location to another in S3.
        :param source_bucket: name of bucket
        :param source_object: name of key
        :param target_bucket: name of bucket
        :param target_object: name of key
        """
        try:
            copy_source = {
                "Bucket": source_bucket,
                "Key": source_object
            }
            self._resource.meta.client.copy(copy_source, target_bucket, target_object)
        except ClientError as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    def download_object(self, bucket_name: str, object_name: str, path: str) -> None:
        """
        Downloads an object from a bucket to the local disk
        :param bucket_name: name of bucket
        :param object_name: name of key
        :param path: download path
        """
        try:
            self._resource.Bucket(bucket_name).download_file(object_name, path)
        except Exception as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise

    def upload_file(self, bucket_name: str, object_name: str, path: str) -> None:
        """
        Uploads a file from local disk to S3.
        :param bucket_name: name of bucket
        :param object_name: name of object
        :param path: local path of the file
        """
        try:
            self._resource.Bucket(bucket_name).upload_file(path, object_name)
        except Exception as e:
            traceback_str = ''.join(traceback.format_tb(e.__traceback__))
            self._logger.error(traceback_str)
            raise
