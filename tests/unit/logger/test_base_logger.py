import unittest
from unittest.mock import patch
from shared_lib.logger.loggers.base_logger import BaseLogger


class TestBaseLogger(unittest.TestCase):

    @patch.object(BaseLogger, '__abstractmethods__', set())
    def setUp(self) -> None:
        self.logger = BaseLogger()

    def test_base_logger_validates_correctly(self):
        log_level = self.logger.validate_log_level('info')
        self.assertEqual(log_level, "INFO")

    def test_base_logger_validator_raises(self):
        self.assertRaises(ValueError, self.logger.validate_log_level, "Wrong")

    def test_default_format(self):
        log_format = self.logger.default_format()
        self.assertEqual(log_format, '%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')

    def test_make_handler_raises(self):
        handler = self.logger.make_handler()
        self.assertEqual(handler, None)