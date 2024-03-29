import io
import os
import sys
import glob
import unittest
import boto3
from moto import mock_logs

from shared_lib.logger import Logger
from shared_lib.logger.loggers.file_logger import FileLogger
from shared_lib.logger.loggers.stdout_logger import StdoutLogger
from shared_lib.aws import AWS


class TestLogger(unittest.TestCase):

    def test_stdout_logger(self):
        stream = io.StringIO()
        logger = Logger(log_name='test-stdout-logger', log_level='debug', out_stream=stream)
        logger.info('Info test')
        value = stream.getvalue()
        text_contains = "Info test" in value
        self.assertEqual(text_contains, True)

    def test_file_logger(self):
        log_name = 'test-file-logger1'
        logger = Logger(
            log_name=log_name,
            log_level='debug',
            attach_default_file_logger=True,
            attach_default_stdout_logger=False
        )
        logger.info('Info test')

        # Retrieve the log file from the logs folder
        log_file = glob.glob(f"{sys.path[1]}/logs/*{log_name}.log")[0]
        with open(log_file, 'r') as f:
            text = f.read()
        os.remove(log_file)

        text_contains = "Info test" in text
        self.assertEqual(text_contains, True)

    def test_file_and_stdout_logger(self):
        stream = io.StringIO()
        log_name = 'test-file-logger2'
        logger = Logger(
            log_name=log_name,
            log_level='debug',
            out_stream=stream,
            attach_default_stdout_logger=True,
            attach_default_file_logger=True
        )
        logger.debug('Debug test')
        value = stream.getvalue()
        text_contains = "Debug test" in value

        # Check the stdout works
        self.assertEqual(text_contains, True)

        # Retrieve the log file from the logs folder
        log_file = glob.glob(f"{sys.path[1]}/logs/*{log_name}.log")[0]
        with open(log_file, 'r') as f:
            text = f.read()
        os.remove(log_file)

        text_contains = "Debug test" in text
        self.assertEqual(text_contains, True)

    def test_attach_multiple_loggers(self):
        stream = io.StringIO()
        log_name = 'test-file-logger-3'
        file_logger = FileLogger(
            log_name=log_name,
            log_level='debug'
        )
        stdout_logger = StdoutLogger(
            log_name='test-logger-2',
            log_level='debug',
            stream=stream
        )
        logger = Logger(
            loggers=[file_logger, stdout_logger]
        )
        logger.warning('Warning test')
        value = stream.getvalue()
        text_contains = "Warning test" in value

        # Check the stdout works
        self.assertEqual(text_contains, True)

        # Retrieve the log file from the logs folder
        log_file = glob.glob(f"{sys.path[1]}/logs/*{log_name}.log")[0]
        with open(log_file, 'r') as f:
            text = f.read()
        os.remove(log_file)

        text_contains = "Warning test" in text
        self.assertEqual(text_contains, True)

    def test_debug_message(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        logger.debug('Debug test')
        value = stream.getvalue()
        text_contains = "Debug test" in value
        self.assertEqual(text_contains, True)

    def test_info_message(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        logger.info('Info test')
        value = stream.getvalue()
        text_contains = "Info test" in value
        self.assertEqual(text_contains, True)

    def test_warning_message(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        logger.warning('Warning test')
        value = stream.getvalue()
        text_contains = "Warning test" in value
        self.assertEqual(text_contains, True)

    def test_error_message(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        logger.error('Error test')
        value = stream.getvalue()
        text_contains = "Error test" in value
        self.assertEqual(text_contains, True)

    def test_critical_message(self):
        stream = io.StringIO()
        logger = Logger(log_level='debug', out_stream=stream)
        logger.critical('Critical test')
        value = stream.getvalue()
        text_contains = "Critical test" in value
        self.assertEqual(text_contains, True)

    def test_attach_logger_raises_with_wrong_class(self):
        class WrongClass:
            pass
        self.assertRaises(RuntimeError, Logger, loggers=[WrongClass])

    @mock_logs
    def test_integration_with_aws_cloudwatch(self):
        client = boto3.client('logs', "eu-west-1")
        aws = AWS(region="eu-west-1", client=client)
        aws_logger = aws.logs(log_group_name="test-log-group", log_stream_name="test-log-stream", log_level='DEBUG')
        aws_logger.create_log_group()
        aws_logger.create_log_stream()
        logger = Logger(
            log_name='test-stdout-logger',
            log_level='INFO',
            loggers=[aws_logger]
        )

        logger.info('Info test')

        # Check the logs to get the message
        response = client.get_log_events(
            logGroupName="test-log-group",
            logStreamName="test-log-stream"
        )

        message = response["events"][0]["message"]
        self.assertEqual("INFO - Info test" in message, True)
