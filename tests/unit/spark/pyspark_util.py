import unittest
import boto3

from tests.unit.spark.moto_server import MotoServer
from shared_lib.spark.pyspark_factory import SparkFactory


class MockSparkSession(unittest.TestCase):

    s3_region = "eu-west-2"
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        """
        The setup class creates the S3 clients and the spark session (assuming running moto s3 server)
        """
        cls._create_aws_clients()
        cls._init_spark_session()

    @classmethod
    def _create_aws_clients(cls):
        # Create s3 connections and urls
        s3_mock_endpoint = f"http://127.0.0.1:{MotoServer.port}"

        print(f"s3_mock_endpoint = {s3_mock_endpoint}")
        cls.s3_resource = boto3.resource(
            "s3",
            region_name=cls.s3_region,
            endpoint_url=s3_mock_endpoint
        )
        cls.s3_client = boto3.client(
            "s3",
            region_name=cls.s3_region,
            endpoint_url=s3_mock_endpoint
        )

    @classmethod
    def _init_spark_session(cls, log_level='INFO'):
        spark_factory = SparkFactory(
            master="local"
        )

        # Set up the config to bypass the real S3 connection in Spark
        config = {
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.2",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": "dummy",
            "spark.hadoop.fs.s3a.secret.key": "dummy",
            "spark.hadoop.fs.s3a.session.token": "dummy",
            "spark.hadoop.fs.s3a.endpoint": f"http://127.0.0.1:{MotoServer.port}",
        }

        cls.spark = spark_factory.create_session(
            app_name="Test session",
            extra_args=config,
            log_level=log_level
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
