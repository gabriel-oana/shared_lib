import sys
import os
import io
import glob
import unittest
import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

from shared_lib.aws import AWS
from shared_lib.logger import Logger


class TestDefaults:
    region = "eu-west-2"
    bucket_name = "TEST-BUCKET-NAME"
    bucket_name2 = "TEST-BUCKET-NAME-2"
    object_name = "TEST-OBJECT-NAME"
    object_name2 = "TEST-OBJECT-NAME-2"


@mock_s3
class TestS3(unittest.TestCase):

    def setUp(self) -> None:
        self.defaults = TestDefaults()
        self.s3 = boto3.client("s3", region_name=self.defaults.region)

        # Create mock buckets
        self.s3.create_bucket(
            Bucket=self.defaults.bucket_name,
            CreateBucketConfiguration={"LocationConstraint": self.defaults.region}
        )

        self.s3.create_bucket(
            Bucket=self.defaults.bucket_name2,
            CreateBucketConfiguration={"LocationConstraint": self.defaults.region}
        )

        self.aws = AWS(
            region=self.defaults.region,
            client=self.s3
        )

    def test_get_object_without_range(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        obj_body = self.aws.s3.get_object(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name)
        self.assertEqual(obj_body, "some-content")

    def test_get_object_with_range(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        obj_body = self.aws.s3.get_object(
            bucket_name=self.defaults.bucket_name,
            object_name=self.defaults.object_name,
            byte_range='bytes=0-1'
        )
        self.assertEqual(obj_body, 'so')

    def test_get_object_raises(self):
        self.assertRaises(ClientError, self.aws.s3.get_object, bucket_name="WRONG-BUCKET-NAME",
                          object_name=self.defaults.object_name)

    def test_put_object_string(self):
        # Put an object there first directly using the boto3 client
        self.aws.s3.put_object(
            bucket_name=self.defaults.bucket_name,
            object_name=self.defaults.object_name,
            body='some-content'
        )
        body = self.s3.get_object(
            Bucket=self.defaults.bucket_name,
            Key=self.defaults.object_name,
        )["Body"].read().decode()

        self.assertEqual(body, "some-content")

    def test_put_object_int(self):
        # Put an object there first directly using the boto3 client
        self.aws.s3.put_object(
            bucket_name=self.defaults.bucket_name,
            object_name=self.defaults.object_name,
            body=12345
        )
        body = self.s3.get_object(
            Bucket=self.defaults.bucket_name,
            Key=self.defaults.object_name,
        )["Body"].read().decode()

        self.assertEqual(body, "12345")

    def test_put_object_raises(self):
        self.assertRaises(ClientError, self.aws.s3.put_object, bucket_name="WRONG-BUCKET-NAME",
                          object_name=self.defaults.object_name, body="content")

    def test_delete_object(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        self.aws.s3.delete_object(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name)
        self.assertRaises(ClientError, self.s3.get_object, Bucket=self.defaults.bucket_name,
                          Key=self.defaults.object_name)

    def test_delete_object_raises(self):
        self.assertRaises(ClientError, self.aws.s3.delete_object, bucket_name="WRONG",
                          object_name=self.defaults.object_name)

    def test_get_object_size(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        size = self.aws.s3.get_object_size(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name)
        self.assertEqual(size, 0.0)

    def test_get_object_size_raises(self):
        self.assertRaises(ClientError, self.aws.s3.get_object_size, bucket_name="WRONG", object_name=self.defaults.object_name)

    def test_get_object_size_in_bytes(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        size = self.aws.s3.get_object_size(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name,
                                           str_format="B")
        self.assertEqual(size, 12.0)

    def test_get_object_size_raises_with_wrong_str_format(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        self.assertRaises(ValueError, self.aws.s3.get_object_size, bucket_name=self.defaults.bucket_name,
                          object_name=self.defaults.object_name, str_format="T")

    def test_get_object_sizes(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name2, Body='some-content')
        size = self.aws.s3.get_objects_size(bucket_name=self.defaults.bucket_name, prefixes=["TEST"], str_format='B')
        self.assertEqual(size, 24.0)

    def test_get_objects_size_raises_with_no_files(self):
        self.assertRaises(RuntimeError, self.aws.s3.get_objects_size, bucket_name=self.defaults.bucket_name,
                          prefixes=["TEST"], str_format='B')

    def test_get_matching_objects(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        matched_objects = self.aws.s3.get_matching_objects(
            bucket_name=self.defaults.bucket_name,
            prefix='TEST'
        )
        expected = [
            {'bucket': self.defaults.bucket_name,
             'file_name': self.defaults.object_name,
             'key': self.defaults.object_name,
             'total_objects_matches': 1}
        ]
        self.assertListEqual(matched_objects, expected)

    def test_get_matching_objects_with_regex_match(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        matched_objects = self.aws.s3.get_matching_objects(
            bucket_name=self.defaults.bucket_name,
            prefix='TEST',
            regex_match=".*"
        )
        expected = [
            {'bucket': self.defaults.bucket_name,
             'file_name': self.defaults.object_name,
             'key': self.defaults.object_name,
             'total_objects_matches': 1}
        ]
        self.assertListEqual(matched_objects, expected)

    def test_get_matching_objects_strict(self):
        self.assertRaises(RuntimeError, self.aws.s3.get_matching_objects, bucket_name=self.defaults.bucket_name,
                          prefix='TEST', strict=True)

    def test_transfer_object(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        self.aws.s3.transfer_object(
            source_object=self.defaults.object_name,
            source_bucket=self.defaults.bucket_name,
            target_object=self.defaults.object_name,
            target_bucket=self.defaults.bucket_name2
        )
        matched_objects = self.aws.s3.get_matching_objects(
            bucket_name=self.defaults.bucket_name2,
            prefix='TEST',
        )
        expected = [
            {'bucket': self.defaults.bucket_name2,
             'file_name': self.defaults.object_name,
             'key': self.defaults.object_name,
             'total_objects_matches': 1}
        ]
        self.assertListEqual(matched_objects, expected)

    def test_transfer_raises(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')
        self.assertRaises(ClientError, self.aws.s3.transfer_object, source_object=self.defaults.object_name,
                          source_bucket=self.defaults.bucket_name, target_object=self.defaults.object_name,
                          target_bucket="WRONG")

    def test_download_file(self):
        # Put an object there first directly using the boto3 client
        self.s3.put_object(Bucket=self.defaults.bucket_name, Key=self.defaults.object_name, Body='some-content')

        file_path = f'{sys.path[1]}/{self.defaults.object_name}'

        self.aws.s3.download_object(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name,
                                    path=file_path)
        file_exists = glob.glob(file_path)
        os.remove(file_path)
        self.assertEqual(len(file_exists), 1)

    def test_download_file_raises(self):
        self.assertRaises(Exception, self.aws.s3.download_object, bucket_name="WRONG", object_name=self.defaults.object_name, path='')

    def test_upload_file(self):
        file_path = f'{sys.path[1]}/{self.defaults.object_name}'

        with open(file_path, 'w') as f:
            f.write('some-content')

        self.aws.s3.upload_file(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name,
                                path=file_path)

        body = self.s3.get_object(
            Bucket=self.defaults.bucket_name,
            Key=self.defaults.object_name,
        )["Body"].read().decode()

        os.remove(file_path)
        self.assertEqual(body, "some-content")

    def test_upload_file_raises(self):
        self.assertRaises(Exception, self.aws.s3.upload_file, bucket_name="WRONG",
                          object_name=self.defaults.object_name, path='')

    def test_logging_is_recorded_when_raising(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        try:
            aws = AWS(
                region=self.defaults.region,
                client=self.s3,
                logger=logger
            )
            aws.s3.upload_file(bucket_name=self.defaults.bucket_name, object_name=self.defaults.object_name, path="")
        except Exception as e:
            print(e)
        value = stream.getvalue()
        text_contains = "Exception" in value
        self.assertEqual(text_contains, True)
