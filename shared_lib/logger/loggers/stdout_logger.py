import logging
from shared_lib.logger.loggers.base_logger import BaseLogger


class StdoutLogger(BaseLogger):

    def __init__(self, log_name: str, log_level: str, stream=None):
        self.log_name = log_name
        self.log_level = log_level
        self.stream = stream

    def make_handler(self):
        """
        Creates a handler for the logger
        """

        formatter = logging.Formatter(self.default_format())
        handler = logging.StreamHandler(stream=self.stream)
        handler.setFormatter(formatter)
        logger = logging.getLogger(self.log_name)
        logger.setLevel(self.validate_log_level(self.log_level))
        logger.addHandler(handler)
        return logger
