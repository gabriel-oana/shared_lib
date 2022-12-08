from shared_lib.aws.s3 import S3
from shared_lib.aws.logs import CloudwatchLogs


class AWS:

    def __init__(self, region: str, client=None, resource=None, logger=None):
        self.region = region
        self.client = client
        self.resource = resource
        self.logger = logger

    @property
    def s3(self) -> S3:
        return S3(region=self.region, client=self.client, resource=self.resource, logger=self.logger)

    def logs(self, log_group_name: str, log_stream_name: str, log_level: str, use_batches: bool = False,
             batch_size: int = 25, max_attempts: int = 10, backoff_multiplier: int = 10) -> CloudwatchLogs:
        return CloudwatchLogs(client=self.client, region=self.region, log_level=log_level,
                              log_group_name=log_group_name, log_stream_name=log_stream_name, use_batches=use_batches,
                              batch_size=batch_size, max_attempts=max_attempts, backoff_multiplier=backoff_multiplier)
