
from tests.unit.spark.pyspark_util import MockSparkSession


class TestSparkSession(MockSparkSession):

    s3_bucket_name = "test-bucket"

    def setUp(self) -> None:
        # Create S3 buckets
        self.s3_client.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": self.s3_region}
        )

    def test_read_write_to_s3(self):
        test_data = [
            {"Day": 1, "Amount": 10},
            {"Day": 2, "Amount": 20}
        ]
        df = self.spark.createDataFrame(test_data)
        df.write.save(
            path=f"s3a://{self.s3_bucket_name}/test.csv",
            overwrite=True,
            header=True,
            format='csv',
            mode='overwrite'
        )

        new_df = self.spark.read.csv(path=f"s3a://{self.s3_bucket_name}/test.csv")
        print(new_df)
