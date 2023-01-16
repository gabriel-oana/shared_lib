import os
import time
import socket
import boto3

from shared_lib.logger.loggers.base_logger import BaseLogger


class CloudwatchLogs(BaseLogger):
    """
    Logging module used to send log messages to AWS Cloudwatch Logs.
    The class has been created with the aim of it being implemented into processes that are distributed.

    In distributed systems, the logging module implements a "batching" system.
    This system enables the log messages to be bundled up and sent in one call to AWS.
    The main reasoning for this procedure is that each call requires a "sequence_token" which specifies the location
    of where the next log message should be placed in the log stream (location by timestamp).
    The token is retrieved via "describe_log_streams" call and is limited by AWS at 5 transactions / second / account / region.

    In a distributed framework, several systems will try to append logs to the same timestamp location resulting in a
    race condition. In other words, when two systems try to write to the same location, an error is raised by the API
    call. To overcome this problem, the logs are batched into one single call. This way, the "describe_log_stream" is
    called only once per message.

    When two or more systems write at the same time a batch of logs, an exponential retrying mechanism has been
    implemented.
    The way this works is as follows:
        - System 1 and 2 both attempt to write a batch of logs to the same log stream at the same time.
        - System 1 will get the correct "sequence_token" and will stream the logs.
        - System 2 will get an incorrect "sequence_token" and will fail to stream the logs.
        - System 2 will go to sleep for "backoff_multiplier" * "attempt_number" seconds.
        - System 2 will refresh the token and retry to send the logs.
        - Any failed system will retry this process for the number of "max_attempts".

    This module can be used to create the log groups and streams as well as adding retention days and tags.
    """

    def __init__(self, region: str, log_group_name: str, log_stream_name: str, log_level: str, use_batches: bool = False,
                 batch_size: int = 25, max_attempts: int = 10, backoff_multiplier: int = 10, client=None):
        self._region = region
        self._client = client if client else boto3.client('logs', region_name=region)
        self._log_group_name = log_group_name
        self._log_stream_name = log_stream_name
        self._log_level = log_level
        self._use_batches = use_batches
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._backoff_multiplier = backoff_multiplier

        self.__batched_messages = 0
        self.__sequence_token = None
        self.__base_event_log = self.__create_base_event_log()

    def make_handler(self, *args, **kwargs):
        """
        Implementation from the inherited class that is not required.
        As result the only thing it does it, it validates the log level.
        """
        self.validate_log_level(self._log_level)
        return self

    def create_log_group(self, retention_days: int = 14, tags: dict = None, raise_if_exists: bool = True) -> None:
        """
        Creates a log group. Automatically adds a retention period.
        :param retention_days: number of days for the logs to be retained
        :param tags: tags to be added to the log group
        :param raise_if_exists: raise an error if the log group exists else pass
        """
        try:
            self._client.create_log_group(logGroupName=self._log_group_name)
            self.__add_retention(retention_days=retention_days)
            if tags:
                self.__add_tags(tags=tags)

        except self._client.exceptions.ResourceAlreadyExistsException:
            if raise_if_exists:
                raise RuntimeError('Cloudwatch log group already exists')

    def create_log_stream(self, raise_if_exists: bool = True) -> None:
        """
        Creates a log stream
        """
        try:
            self._client.create_log_stream(logGroupName=self._log_group_name, logStreamName=self._log_stream_name)
        except self._client.exceptions.ResourceAlreadyExistsException:
            if raise_if_exists:
                raise RuntimeError('Cloudwatch log stream already exists')

    def flush(self) -> None:
        """
        Sends the rest of the messages stored which did not complete a full batch.
        """
        if self.__batched_messages > 0:
            self.__send_log_with_retry()
            self.__batched_messages = 0
            self.__base_event_log["logEvents"] = []

    def _handler(self, message: str):
        """
        The role of the handler is to determine which method of inserting logs is used.
        If the batching method is used then it will perform the following:
            - Append the log message to the base log event parameter.
            - Check the number of log events.
            - If the number of log events is max then it will send them to AWS.
            - On success, it will reset the base_event_log and the number of batched_messages.
        Else:
            - It will send a single log event to cloudwatch.
        """
        if self._use_batches:
            self.__append_log_events(message=message)
            self.__batched_messages += 1

            if self.__batched_messages == self._batch_size:
                response = self.__send_log_with_retry()
                self.__batched_messages = 0
                return response
        else:
            self.__append_log_events(message)
            response = self.__send_log_with_retry()
            self.__base_event_log["logEvents"] = []
            return response

    def __send_log(self) -> dict:
        """
        Puts the log event to AWS.
        """
        if self.__sequence_token:
            self.__base_event_log.update({"sequenceToken": self.__sequence_token})
        response = self._client.put_log_events(**self.__base_event_log)
        return response

    def __send_log_with_retry(self) -> dict:
        """
        This is the most intricate part of this class
        The main functionality is to retry sending the batches / single log message to AWS using the retrying system.
        """
        attempt = 0
        while attempt <= self._max_attempts:
            attempt += 1
            waiting_seconds = attempt * self._backoff_multiplier
            try:
                response = self.__send_log()

                # Check if request is valid
                if self.__request_is_valid(response):
                    self.__sequence_token = response["nextSequenceToken"]
                    return response
            except Exception as exc:
                if attempt > self._max_attempts:
                    raise ValueError(f'Max attempts reached - {str(exc)}')
                else:
                    time.sleep(waiting_seconds)
                    self.__refresh_sequence_token()

    def __refresh_sequence_token(self) -> None:
        """
        Refreshes the token whenever necessary and updates the token parameter part of the class.
        """
        response = self._client.describe_log_streams(
            logGroupName=self._log_group_name,
            logStreamName=self._log_stream_name
        )

        # First log in the log stream does not require a sequence token
        if "uploadSequenceToken" in response["logStreams"][0]:
            self.__sequence_token = response["logStreams"][0]["uploadSequenceToken"]

    @staticmethod
    def __request_is_valid(response: dict) -> bool:
        """
        Validates the API request response
        """
        http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        contains_next_sequence_token = "nextSequenceToken" in response.keys()
        if http_status_code == 200 and contains_next_sequence_token is True:
            return True
        else:
            return False

    def __create_base_event_log(self) -> dict:
        """
        Creates a local storage place for all the logs to be populated in a batch.
        """
        base_event_log = {
            "logGroupName": self._log_group_name,
            "logStreamName": self._log_stream_name,
            "logEvents": []
        }
        return base_event_log

    def __append_log_events(self, message: str):
        """
        Appends the log messages when batching is used.
        :param message: log message
        """
        log_event = {
            "timestamp": int(round(time.time() * 1000)),
            "message": f'{self.__get_ip_address()} - PID:{os.getpid()} - {message}'
        }
        self.__base_event_log['logEvents'].append(log_event)

    @staticmethod
    def __get_ip_address() -> str:
        """
        Returns the ipv4 address of the executor to be placed in the log message
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]

    def __should_record_log(self, log_level: str) -> bool:
        """
        Determines where the logs should be captured or ignored.
        This is a crude implementation of logging.
        :param log_level: DEBUG, INFO etc
        """
        level_lookup = {
            "CRITICAL": 50,
            "ERROR": 40,
            "WARNING": 30,
            "WARN": 30,
            "INFO": 20,
            "DEBUG": 10
        }

        if self._log_level.upper() not in level_lookup.keys():
            raise ValueError(f'Log level incorrect. Choose from {list(level_lookup.keys())}')

        return level_lookup[log_level.upper()] >= level_lookup[self._log_level.upper()]

    def __add_retention(self, retention_days: int) -> None:
        """
        Adds the retention policy to the log group.
        :param retention_days: int
        """
        allowed_retention_days = [1, 3, 5, 7, 14, 30, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653]
        if retention_days not in allowed_retention_days:
            raise ValueError(f"Retention days for cloudwatch logs must be one of {allowed_retention_days}")
        else:
            self._client.put_retention_policy(
                logGroupName=self._log_group_name,
                retentionInDays=retention_days
            )

    def __add_tags(self, tags: dict):
        """
        Adds tags to the log group
        :param tags: dict
        """
        self._client.tag_log_group(
            logGroupName=self._log_group_name,
            tags=tags
        )

    def debug(self, message: str) -> None:
        log_level = 'DEBUG'
        log_message = f"{log_level} - {message}"
        if self.__should_record_log(log_level):
            self._handler(log_message)

    def info(self, message: str) -> None:
        log_level = 'INFO'
        log_message = f"{log_level} - {message}"
        if self.__should_record_log(log_level):
            self._handler(log_message)

    def warning(self, message: str) -> None:
        log_level = 'WARNING'
        log_message = f"{log_level} - {message}"
        if self.__should_record_log(log_level):
            self._handler(log_message)

    def error(self, message: str) -> None:
        log_level = 'ERROR'
        log_message = f"{log_level} - {message}"
        if self.__should_record_log(log_level):
            self._handler(log_message)

    def critical(self, message: str) -> None:
        log_level = 'CRITICAL'
        log_message = f"{log_level} - {message}"
        if self.__should_record_log(log_level):
            self._handler(log_message)

