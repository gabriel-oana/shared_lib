from logger import AWSBase


class S3(AWSBase):
    """
    Description:
        Module to interact with S3 buckets / objects
    """
    def test(self):
        print(self.client)


if __name__ == '__main__':
    aws_b = AWSBase(client='bob')
    aws = S3()
    aws.test()

