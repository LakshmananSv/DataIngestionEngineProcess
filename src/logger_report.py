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
    
    def form_table(self,house,start_time,end_time,ee_missing_minutes,total_time_minutes,minutes):
        data = {'product_name': ['laptop', 'printer', 'tablet', 'desk', 'chair'],'price': [1200, 150, 300, 450, 200]}
        df = pd.DataFrame(data)
        print (df)
        


