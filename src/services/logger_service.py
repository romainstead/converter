import logging
import os
from datetime import datetime


class LoggingService:
    def __init__(self, log_level=logging.INFO, task_name="DefaultTask"):
        logs_base_dir = os.getenv("LOGS_BASE_DIR", os.path.join(os.getcwd(), "logs"))
        os.makedirs(logs_base_dir, exist_ok=True)
        self.task_name = task_name
        self.task_log_directory = os.path.join(logs_base_dir, self.task_name)
        os.makedirs(self.task_log_directory, exist_ok=True)
        log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"
        log_path = os.path.join(self.task_log_directory, log_filename)
        self.logger = logging.getLogger(f"{__name__}.{self.task_name}")
        self.logger.setLevel(log_level if log_level != logging.DEBUG else logging.INFO)
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(console_handler)

    def info(self, message: str): self.logger.info(message)
    def warning(self, message: str): self.logger.warning(message)
    def error(self, message: str): self.logger.error(message)
    def debug(self, message: str): self.logger.debug(message)
    def critical(self, message: str): self.logger.critical(message)
    def exception(self, message: str): self.logger.exception(message)
