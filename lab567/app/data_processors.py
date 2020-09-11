from app import app
from urllib.request import urlopen
import json
from app.loggers import context
from app.redis_init import r
from app.constants import Config


class RedisController:

    @classmethod
    def set(cls, data, value):
        if Config.WRITE_TO_REDIS:
            r.set(data, value)

    @classmethod
    def get(cls, topic):
        if Config.WRITE_TO_REDIS:
            return r.get(topic)
        else:
            return ""

    @classmethod
    def log(cls, data):
        if Config.WRITE_TO_REDIS:
            context.do_logging(data)
        else:
            return ""


class DataProcessor:

    def __init__(self, url, rows_number, rows_per_time, offset):
        self.base_url = url
        self.limit = rows_per_time
        self.rows_number = rows_number
        self.offset = offset

    def get_json_part(self, offset):
        url = f'{self.base_url}?$limit={self.limit}&$offset={offset}'
        response = urlopen(url)
        data = json.load(response)
        RedisController.log(f"rows {offset}-{offset+self.limit} added")
        RedisController.set(
            f"file {self.base_url} rows {offset}-{offset+self.limit}", "Completed")
        return data
    
    def process_json_rows(self, data):
        for value in data:
            app.logger.info(value)
            context.do_logging(json.dumps({"body": value}))
            
    def get_data_from_url(self, offset):
        data = self.get_json_part(offset)
        self.process_json_rows(data)
        return data

    def load_data(self):
        RedisController.log("Started loading data from: " + self.base_url)
        RedisController.set(f"file {self.base_url}", "Started")
        offset = 0
        self.data_list = []
        while (offset < self.rows_number):
            data = self.get_data_from_url(offset)
            offset += self.offset
            self.data_list += data
        RedisController.set(f"file {self.base_url}", "Completed")
        return self.data_list

    def loop_data(self):
        try:
            self.load_data()
        except Exception:
            self.load_data()

    def check_in_redis(self):
        try:
            does_file_exists = RedisController.get(f"file {self.base_url}")
            if does_file_exists == b"Completed":
                RedisController.set(
                    f"file {self.base_url}", "Retry attempt, ignore a file")
                return "Retry attempt, ignore a file"
            elif does_file_exists is None:
                self.load_data()
        except Exception:
            self.load_data()
