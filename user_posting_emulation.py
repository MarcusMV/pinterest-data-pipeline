
import requests
from time import sleep
import random
import json
import sqlalchemy
from sqlalchemy import text
import argparse
from dotenv import load_dotenv

load_dotenv()
import os

class AWSDBConnector:
    """
    A class representing a connector to an AWS RDS database.
    """
    def __init__(self):
        self.HOST = os.environ.get('DB_HOST')
        self.USER = os.environ.get('DB_USER')
        self.PASSWORD = os.environ.get('DB_PASSWORD')
        self.DATABASE = os.environ.get('DB_DATABASE')
        self.PORT = os.environ.get('DB_PORT')
        
    def create_db_connector(self):
        """
        Creates and returns a database engine for connecting to the AWS RDS database.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


class DataIngester:
    """
    Class responsible for ingesting data and posting it to a specified destination.
    """
    def __init__(self, connector, streaming):
        self.connector = connector
        self.streaming = streaming
        self.base_url = os.environ.get('INVOKE_BASE_URL')

    def run_infinite_post_data_loop(self):
        """
        Runs an infinite loop to continuously post data to the specified destination.
        """
        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = self.connector.create_db_connector()

            # tables = ['pinterest_data', 'geolocation_data', 'user_data']
            tables = {
                'pinterest_data': 'pin',
                'geolocation_data': 'geo',
                'user_data': 'user'
            }

            with engine.connect() as connection:

                for k, v in tables.items():
                    query = text(f"SELECT * FROM {k} LIMIT {random_row}, 1")
                    selected_row = connection.execute(query)
                    for row in selected_row:
                        result = dict(row._mapping)

                        # Publish data to the specified topics
                        if self.streaming:
                            self._post_data_streaming(result, v)
                        else:
                            self._post_data(result, v)

                    print(f'{k}: {result}')

            engine.dispose()

    def _post_data(self, data, topic):
        """
        Posts the data to the specified destination using HTTP POST request.
        
        Args:
            data (dict): The data to be posted.
            topic (str): The topic or destination to post the data to.
        """
        invoke_url = f"{self.base_url}/topics/12c5e9eb47cb.{topic}"

        payload = json.dumps({
            "records": [
                {
                    "value": data
                }
            ]
        }, default=str)  

        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.request("POST", invoke_url, headers=headers, data=payload)

        print(response.status_code)

    def _post_data_streaming(self, data, topic):
        """
        Posts the data to the specified destination using HTTP PUT request for streaming data.
        
        Args:
            data (dict): The data to be posted.
            topic (str): The topic or destination to post the data to.
        """
        invoke_url = f"{self.base_url}/streams/streaming-12c5e9eb47cb-{topic}/record"

        payload = json.dumps({
            "StreamName": f"streaming-12c5e9eb47cb-{topic}",
            "Data": data,
            "PartitionKey": "partition-2"
        }, default=str)

        headers = {'Content-Type': 'application/json'}
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)

        print(response.text)



if __name__ == "__main__":
    print("Starting the data ingester...")
    # 'python user_posting_emulation.py [--streaming]'
    parser = argparse.ArgumentParser(description="Run the data ingester.")
    parser.add_argument("--streaming", action="store_true", help="Enable streaming mode")
    args = parser.parse_args()

    new_connector = AWSDBConnector()
    ingester = DataIngester(new_connector, streaming=args.streaming)
    ingester.run_infinite_post_data_loop()
    
    


