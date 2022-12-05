from shared_lib.aws.s3 import S3


class AWS:

    def __init__(self, region: str, client=None, resource=None, logger=None):
        self.region = region
        self.client = client
        self.resource = resource
        self.logger = logger

    @property
    def s3(self) -> S3:
        return S3(region=self.region, client=self.client, resource=self.resource, logger=self.logger)




