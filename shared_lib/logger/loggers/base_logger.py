from abc import ABC, abstractmethod


class BaseLogger(ABC):

    @staticmethod
    def validate_log_level(log_level: str) -> str:
        """
        Validates the log level string and returns the correct string
        :param log_level: "debug", "DEBUG"
        :return: "debug"
        """
        valid_levels = ['debug', 'info', 'warning', 'error', 'critical']
        if not isinstance(log_level, str) or log_level.lower() not in valid_levels:
            raise ValueError(f'Incorrect log level type. Accepted types: {valid_levels}')
        else:
            return log_level.upper()

    @staticmethod
    def default_format() -> str:
        return "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"

    @abstractmethod
    def make_handler(self, *args, **kwargs):
        pass
