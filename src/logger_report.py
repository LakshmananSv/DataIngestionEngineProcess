import logging
import pandas as pd

class LoggerReport:
    def __init__(self) -> None:
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.message = ""
        self.summary_message = ""

    def info(self, string):
        self.logger.info(string)
        self.message += f"{string}\n"

    def warning(self, string):
        self.logger.warning(string)
        self.message += f"[WARNING] {string}\n"

    def error(self, string):
        self.logger.error(string)
        self.message += f"[ERROR] {string}\n"

    def append_to_summary_message(self, string):
        self.summary_message += f"{string}\n"


