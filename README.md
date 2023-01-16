# Shared Library

- [Shared Library](#shared-library)
    + [1. Description](#1-description)
    + [2. Installation](#2-installation)
    + [3. Contents](#3-contents)
      - [3.1 AWS](#31-aws)
        * [3.1.1 S3](#311-s3)
      - [3.1.2 Cloudwatch Logs](#312-cloudwatch-logs)
        * [Simple Usage](#simple-usage)
        * [Distributed usage](#distributed-usage)
      - [3.2 Logger](#32-logger)
        * [3.2.1 Output to stdout](#321-output-to-stdout)
        * [3.2.2 Output to file](#322-output-to-file)
        * [3.2.3 Output to AWS Cloudwatch Logs](#323-output-to-aws-cloudwatch-logs)
        * [3.2.4 Output to multiple places](#324-output-to-multiple-places)
        * [3.2.5 Extending the logger class](#325-extending-the-logger-class)
    + [3.3 PySpark](#33-pyspark)

### 1. Description
The purpose of this library is to combine various parts of shared functionality that is used 
across my personal projects.     
Generally speaking, now modifications will exist that will break the existing functionality but extra 
functionality is to be added whenever necessary. 

### 2. Installation
For installation purposes, please use SSH. 
```
pip3 install git+ssh://git@github.com:gabriel-oana/shared_lib.git
```

### 3. Contents

#### 3.1 AWS
This module contains all the wrapper for AWS functionality. To be expanded whenever a new service is required.

##### 3.1.1 S3
The S3 class allows a user to interact with S3 in a safe manner. 
It also provides some extra functionality that is not available through the boto API. 

```python
from shared_lib.aws import AWS


aws = AWS(region='eu-west-1')

# Get object
obj = aws.s3.get_object(
    bucket_name='test-bucket',
    object_name='test-object'
)

# Put object
aws.s3.put_object(
    bucket_name='test-bucket',
    object_name='test-object',
    body='data'
)

# Delete object
aws.s3.delete_object(
    bucket_name='test-bucket',
    object_name='test-object'
)

# Get object size - 1 single object
size = aws.s3.get_object_size(
    bucket_name='test-bucket',
    object_name='test-object',
    str_format="MB"
)

# Get objects size - multiple objects
sizes = aws.s3.get_objects_size(
    bucket_name='test-bucket',
    prefixes=['test1', 'test2'],
    str_format="MB"
)

# Get matching objects - gets a list of all the matched objects with prefix/suffix and/or regex
matched_objects = aws.s3.get_matching_objects(
    bucket_name='test-bucket',
    prefix='test',
    suffix='.csv',
    regex_match=None,
    strict=False
)

# Transfer object from one S3 path to another S3 path
aws.s3.transfer_object(
    source_bucket='bucket-1',
    source_object="file-1",
    target_bucket="bucket-2",
    target_object="file-2"
)

# Download files locally
aws.s3.download_object(
    bucket_name='test-bucket',
    object_name='test-object',
    path='local-path'
)

# Upload files locally
aws.s3.upload_file(
    bucket_name='test-bucket',
    object_name='test-object',
    path='local-path'
)
```

#### 3.1.2 Cloudwatch Logs
This class can be used on its own to access only cloudwatch logs, but it can also be accessed 
through the Logger class that is showcased below.

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

##### Simple Usage

```python
from shared_lib.aws import AWS

aws = AWS(region='eu-west-1')

logger = aws.logs(
    log_group_name="my-log-group",
    log_stream_name="my-log-stream",
    log_level="INFO"
)

# Create log group
logger.create_log_group(
    retention_days=3,
    tags={"my-tag": "tag1"},
    raise_if_exists=False
)

# Create log stream
logger.create_log_stream()

logger.info('my message')
```

##### Distributed usage
Logs are batched before sending them to Cloudwatch.    
This method is capable of sending large number of messages using fewer API calls.

```python
from shared_lib.aws.logs import CloudwatchLogs

logger = CloudwatchLogs(
    region='eu-west-1',
    log_group_name="my-log-group",
    log_stream_name="my-log-stream",
    log_level="INFO",
    use_batches=True,
    batch_size=2,
    max_attempts=10,
    backoff_multiplier=2
)

logger.info('msg1')
logger.info('msg2')
logger.info('msg3') # <- This will not be sent since batch_size = 2

logger.flush() # This will send all the logs stored in the class's cache. 
               # In this case it will send msg3 since 1 and 2 were sent.
```

#### 3.2 Logger
The logger class supports many types of loggers combined as one.      
The class accepts multiple loggers and streams the messages to the attached loggers.
Creating a new logger class must inherit the BaseLogger (this is shown below).

##### 3.2.1 Output to stdout
```python
from shared_lib.logger import Logger

logger = Logger(
    log_level='debug',
    attach_default_stdout_logger=True,
    attach_default_file_logger=False
)
logger.debug('Debug test')
```

##### 3.2.2 Output to file
```python
from shared_lib.logger import Logger

logger = Logger(
    log_name="output",
    log_level='debug',
    log_path="my-log-path",
    attach_default_stdout_logger=False,
    attach_default_file_logger=True
)
logger.debug('Debug test')
```

##### 3.2.3 Output to AWS Cloudwatch Logs
```python
from shared_lib.logger import Logger
from shared_lib.aws import AWS

aws = AWS(region="eu-west-1")
aws_logger = aws.logs(log_group_name="test-log-group", log_stream_name="test-log-stream", log_level='DEBUG')
aws_logger.create_log_group()
aws_logger.create_log_stream()

logger = Logger(
    log_level='debug',
    loggers=[aws_logger]
)
logger.debug('Debug test')
```

##### 3.2.4 Output to multiple places
This method will show how to attach multiple loggers to and combine them into one.

```python
from shared_lib.logger import Logger
from shared_lib.logger.loggers.stdout_logger import StdoutLogger
from shared_lib.logger.loggers.file_logger import FileLogger
from shared_lib.aws import AWS


log_name = "log-name"
log_level = "DEBUG"

# AWS Logger
aws = AWS(region="eu-west-1")
aws_logger = aws.logs(log_group_name="test-log-group", log_stream_name=log_name, log_level=log_level)
aws_logger.create_log_group()
aws_logger.create_log_stream()

file_logger = FileLogger(
    log_name=log_name,
    log_level=log_level,
    log_path="my-path"
)
stdout_logger = StdoutLogger(
    log_name=log_name,
    log_level=log_level,
)

logger = Logger(
    log_level=log_level,
    loggers=[aws_logger, file_logger, stdout_logger]
)
logger.debug('Debug test')
```

##### 3.2.5 Extending the logger class
Each additional logger must inherit the BaseLogger class.
For example, let's say that we want a logger that only prints the log message.

```python
from shared_lib.logger.loggers.base_logger import BaseLogger
from shared_lib.logger import Logger

# This class must inherit the base logger and implement "make_handler" abstract method. 
# It must also implement the "debug, info, warning, error, critical" methods although they are not enforced. (TODO)
class PrintLogger(BaseLogger):
    
    def make_handler(self, message: str):
        print(message)

    def info(self, message):
        self.make_handler(message)


print_logger = PrintLogger()        
        

logger = Logger(
    log_level="DEBUG",
    loggers=[print_logger]
)
        
logger.info('message')
```


### 3.3 PySpark
Pyspark factory creates a spark session in an "easier" manner which allows one to add more parameters dynamically. 
The most important aspect of this class is not how to create a spark session but how to perform a unit-test on a 
PySpark session. For reference please check the test available for PySpark.

```python
from shared_lib.spark.pyspark_factory import SparkFactory

# Create spark session with specific parameters
config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.2",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": "dummy",
    "spark.hadoop.fs.s3a.secret.key": "dummy",
    "spark.hadoop.fs.s3a.session.token": "dummy",
    "spark.hadoop.fs.s3a.endpoint": f"http://127.0.0.1:5000",
}

spark_factory = SparkFactory(
    master="local[*]"
)

spark = spark_factory.create_session(
    app_name='demo',
    executor_memory='2g',
    executor_cores=4,
    default_parallelism=4,
    total_executor_cores=16,
    num_executors=4,
    log_level='INFO',
    extra_args=config
)
```