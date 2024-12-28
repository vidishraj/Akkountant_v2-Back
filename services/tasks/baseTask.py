import json
import os
import shutil
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import requests

from models.Jobs import Job
from services import JsonDownloadService
from utils.logger import Logger


# The task is to give a row. The row should be updated
# with the task results. The status should be updated and the next row should be added with the execution time.
# 10 failures should turn the last task off

class BaseTask(ABC):
    """Abstract base class for a scheduled task."""
    _instance = None  # Singleton instance
    id: int = None
    title: str
    result: str
    priority: str
    status: str
    due_date: datetime
    user_id: str = None

    tmp_dir: str = os.getcwd() + '/task_tmp/'

    # Unique for every task
    interval: int

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(BaseTask, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            # Initialise singleton with only title and priority
            self.title = title
            self.jsonService = JsonDownloadService.JSONDownloadService(os.getcwd() + 'services/assets')
            self.priority = priority
            # Make tmp_dir if it doesnt exist
            os.makedirs(self.tmp_dir, exist_ok=True)

    def init_runner(self, row: Job):
        self.id = row.id
        self.status = row.status
        self.due_date = row.due_date
        self.user_id = row.user_id

    def startTask(self):
        result, status, interval = self.run()
        return result, status, interval

    @abstractmethod
    def run(self):
        """Method containing the task logic. Must be overridden by subclasses."""
        pass

    @staticmethod
    def make_request(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()  # Parse the JSON response

    def save_json(self, data, file_path):
        """
        Saves the provided data to a JSON file.
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            self.logger.info(f"File saved successfully at: {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")

    @staticmethod
    def move_file(source_path: str, destination_path: str) -> bool:
        """
        Moves a file from the source directory to the destination directory.

        Parameters:
            source_path (str): The full path of the file to move.
            destination_path (str): The directory to move the file to.

        Returns:
            bool: True if the file was moved successfully, False otherwise.
        """
        try:
            # Check if the source file exists
            if not os.path.isfile(source_path):
                print(f"Source file does not exist: {source_path}")
                return False
            # Move the file
            shutil.move(source_path, destination_path)
            return True
        except Exception:
            return False
