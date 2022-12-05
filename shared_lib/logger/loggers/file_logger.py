import sys
import os
import logging
from pathlib import Path
from datetime import datetime

from shared_lib.logger.loggers.base_logger import BaseLogger


class FileLogger(BaseLogger):

    def __init__(self, log_name: str, log_level: str, log_path: str = None, write_mode: str = 'a'):
        self.log_name = log_name
        self.log_level = log_level
        self.log_path = log_path
        self.write_mode = write_mode

    def __make_logs_folder(self) -> str:
        """
        Creates a log directory with a log file to which all the application log is written to
        """
        log_path = sys.path[1]

        location_exists = os.path.exists(f"{log_path}/logs")
        if not location_exists:
            Path(f"{log_path}/logs").mkdir(parents=False, exist_ok=True)

        today = datetime.now().strftime("%Y-%m-%d")
        log_file_name = f'{log_path}/logs/{today}_{self.log_name}.log'
        return log_file_name

    def make_handler(self):
        """
        Creates a handler for the logger
        """
        if not self.log_path:
            log_path = self.__make_logs_folder()
            log_file = f"{log_path}"
        else:
            log_file = f"{self.log_path}/{self.log_name}"

        formatter = logging.Formatter(self.default_format())
        handler = logging.FileHandler(log_file, mode=self.write_mode)
        handler.setFormatter(formatter)
        logger = logging.getLogger('file_logger')
        logger.setLevel(self.validate_log_level(self.log_level))
        logger.addHandler(handler)
        return logger
