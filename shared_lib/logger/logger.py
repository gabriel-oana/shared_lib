from typing import List
from shared_lib.logger.loggers.base_logger import BaseLogger
from shared_lib.logger.loggers.stdout_logger import StdoutLogger
from shared_lib.logger.loggers.file_logger import FileLogger


class Logger:

    def __init__(
            self,
            log_name: str = None,
            log_level: str = None,
            log_path: str = None,
            loggers: List[BaseLogger] = None,
            attach_default_file_logger: bool = False,
            attach_default_stdout_logger: bool = True,
            out_stream=None
    ):
        """
        :param log_name: Name of the logger
        :param log_level: Log level (debug, info, warning, error, critical)
        :param log_path: Path of the logging file if required
        :param loggers: List of initialised loggers. Must be an instance of the BaseLogger class
        :param out_stream: A stream to be used to capture all the logs from the Stdout logs
        """

        self.log_name = log_name
        self.log_level = log_level
        self.log_path = log_path
        self.attach_default_file_logger = attach_default_file_logger
        self.attach_default_stdout_logger = attach_default_stdout_logger
        self.out_stream = out_stream
        self.log_register = []

        if loggers:
            self.__attach_loggers(loggers)
        else:
            if self.attach_default_stdout_logger:
                self.__attach_default_stdout_logger()

            if self.attach_default_file_logger:
                self.__attach_default_file_logger()

    def __attach_loggers(self, loggers: List[BaseLogger]):
        """
        Verifies the logger if an instance of the base class and attaches it to the log registers
        """

        for logger in loggers:
            if isinstance(logger, BaseLogger):
                handler = logger.make_handler()
                self.log_register.append(handler)
            else:
                raise RuntimeError("Attached logger must be an instance of the BaseLogger class")

    def __attach_default_file_logger(self):
        """
        Attaches the default file logger
        """
        logger = FileLogger(log_name=self.log_name, log_level=self.log_level)
        handler = logger.make_handler()
        self.log_register.append(handler)

    def __attach_default_stdout_logger(self):
        """
        Attaches the default stdout logger
        """
        logger = StdoutLogger(log_name=self.log_name, log_level=self.log_level, stream=self.out_stream)
        handler = logger.make_handler()
        self.log_register.append(handler)

    def debug(self, message: str):
        for handler in self.log_register:
            handler.debug(message)

    def info(self, message: str):
        for handler in self.log_register:
            handler.info(message)

    def warning(self, message: str):
        for handler in self.log_register:
            handler.warning(message)

    def error(self, message: str):
        for handler in self.log_register:
            handler.error(message)

    def critical(self, message: str):
        for handler in self.log_register:
            handler.critical(message)
