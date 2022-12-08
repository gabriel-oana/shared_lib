import unittest
import boto3
from moto import mock_logs
from botocore.exceptions import ClientError

from shared_lib.aws import AWS


class Defaults:
    region = 'eu-west-1'
    log_group_name = "TEST-GROUP"
    log_stream_name = "TEST-STREAM"
    log_level = 'INFO'
    retention_days = 3
    max_attempts = 2
    backoff_multiplier = 5
    tags = {"test": "abc"}


@mock_logs
class TestCloudwatchLogs(unittest.TestCase):

    def setUp(self) -> None:
        self.defaults = Defaults()
        self.client = boto3.client('logs', self.defaults.region)
        self.aws = AWS(region=self.defaults.region, client=self.client)
        self.logs = self.aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level=self.defaults.log_level
        )

    def test_log_group_is_created(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)

        response = self.client.describe_log_groups()
        number_of_groups = len(response['logGroups'])
        self.assertEqual(number_of_groups, 1)

    def test_log_group_creation_skips_with_flag(self):
        # Create the log group twice with the raise flag enabled.
        self.logs.create_log_group(raise_if_exists=True)
        self.assertRaises(RuntimeError, self.logs.create_log_group)

    def test_log_stream_is_created(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()
        self.logs.warning('Test')

        response = self.client.describe_log_streams(
            logGroupName=self.defaults.log_group_name,
            logStreamNamePrefix=self.defaults.log_stream_name
        )
        stream_name = response["logStreams"][0]["logStreamName"]
        self.assertEqual(self.defaults.log_stream_name, stream_name)

    def test_log_stream_creation_skips_with_flag(self):
        # Create the log group twice with the raise flag enabled.
        self.logs.create_log_group()
        self.logs.create_log_stream(raise_if_exists=True)
        self.assertRaises(RuntimeError, self.logs.create_log_stream)

    def test_log_retention_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.warning("Test")

        response = self.client.describe_log_groups()
        retention = response["logGroups"][0]["retentionInDays"]
        self.assertEqual(self.defaults.retention_days, retention)

    def test_log_tags_are_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.warning("Test")

        response = self.client.list_tags_log_group(logGroupName=self.defaults.log_group_name)
        tags = response["tags"]
        self.assertEqual(tags, self.defaults.tags)

    def test_log_record_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.warning("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        message = response["events"][0]["message"]
        self.assertEqual("WARNING - Test" in message, True)

    def test_log_record_debug_is_not_captured(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.debug("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )
        messages = len(response["events"])
        self.assertEqual(messages, 0)

    def test_log_info_record_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.info("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        message = response["events"][0]["message"]
        self.assertEqual("INFO - Test" in message, True)

    def test_log_warning_record_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.warning("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        message = response["events"][0]["message"]
        self.assertEqual("WARNING - Test" in message, True)

    def test_log_error_record_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.error("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        message = response["events"][0]["message"]
        self.assertEqual("ERROR - Test" in message, True)

    def test_log_critical_record_is_correct(self):
        self.logs.create_log_group(retention_days=self.defaults.retention_days, tags=self.defaults.tags)
        self.logs.create_log_stream()

        self.logs.critical("Test")

        response = self.client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        message = response["events"][0]["message"]
        self.assertEqual("CRITICAL - Test" in message, True)

    def test_send_log_batch(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level=self.defaults.log_level,
            batch_size=3,
            use_batches=True
        )
        logs.create_log_group()
        logs.create_log_stream()

        logs.info('Message 1')
        logs.info('Message 2')
        logs.info('Message 3')

        response = client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        messages = len(response["events"])
        self.assertEqual(messages, 3)

    def test_send_log_batch_incomplete(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level=self.defaults.log_level,
            batch_size=5,
            use_batches=True
        )
        logs.create_log_group()
        logs.create_log_stream()

        logs.info('Message 1')
        logs.info('Message 2')
        logs.info('Message 3')

        response = client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        messages = len(response["events"])
        self.assertEqual(messages, 0)

    def test_send_log_batch_incomplete_with_flush(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level=self.defaults.log_level,
            batch_size=5,
            use_batches=True
        )
        logs.create_log_group()
        logs.create_log_stream()

        logs.info('Message 1')
        logs.info('Message 2')
        logs.info('Message 3')

        logs.flush()

        response = client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        messages = len(response["events"])
        self.assertEqual(messages, 3)

    def test_send_log_debug(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level="DEBUG",
            batch_size=5,
            use_batches=True
        )
        logs.create_log_group()
        logs.create_log_stream()

        logs.debug('Message 1')
        logs.flush()

        response = client.get_log_events(
            logGroupName=self.defaults.log_group_name,
            logStreamName=self.defaults.log_stream_name
        )

        messages = len(response["events"])
        self.assertEqual(messages, 1)

    def test_send_log_raises_with_wrong_log_level(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level="WRONG_LEVEL",
            batch_size=5,
            use_batches=True
        )
        logs.create_log_group()
        logs.create_log_stream()

        self.assertRaises(ValueError, logs.debug, 'Test')

    def test_send_log_raises_with_wrong_retention_days(self):
        client = boto3.client('logs', self.defaults.region)
        aws = AWS(region=self.defaults.region, client=client)
        logs = aws.logs(
            log_group_name=self.defaults.log_group_name,
            log_stream_name=self.defaults.log_stream_name,
            log_level="WRONG_LEVEL",
            batch_size=5,
            use_batches=True
        )
        self.assertRaises(ValueError, logs.create_log_group, retention_days=4)
