from pyspark.sql import SparkSession


class SparkFactory:

    def __init__(self, master: str):
        self.master = master

    def create_session(self, app_name: str = 'PySpark Session', executor_memory: str = "2g", executor_cores: int = 1,
                       default_parallelism: int = 4, total_executor_cores: int = 2, num_executors: int = 2,
                       log_level: str = 'INFO', extra_args: dict = None) -> SparkSession:
        """
        Creates a spark session.
        :param app_name: Name of the application
        :param executor_memory:
        :param executor_cores:
        :param default_parallelism:
        :param total_executor_cores:
        :param num_executors:
        :param log_level: DEBUG, INFO, WARNING, ERROR
        :param extra_args: eg: {"spark.hadoop.fs.s3a.endpoint": "s3.eu-west-1.amazonaws.com"}
        :return: Spark session
        """

        builder = SparkSession.builder\
            .appName(app_name)\
            .master(self.master)\
            .config("spark.executor.memory", executor_memory)\
            .config("spark.executor.cores", executor_cores)\
            .config("spark.default.parallelism", default_parallelism)\
            .config("spark.cores.max", total_executor_cores)\
            .config("spark.executor.instances", num_executors)

        # Create the extra args if any
        if extra_args:
            for key, value in extra_args.items():
                builder = builder.config(key, value)

        spark_session = builder.getOrCreate()
        spark_session.sparkContext.setLogLevel(log_level)
        return spark_session
