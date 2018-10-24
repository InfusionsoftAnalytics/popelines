import json
from google.cloud import bigquery
import os
import logging
import requests
import sys
import datetime

class popeline:
    """
    Popeline creates a data pipeline for Google's BigQuery. 
    """
    def __init__(self, dataset_id, service_key_file_loc):
        self.bq_client = bigquery.Client.from_service_account_json(service_key_file_loc)

        # set up a logger
        self.log = self.get_logger(level=logging.INFO)

        # get local directory
        self.directory = str(os.path.abspath(os.path.dirname(__file__)))

        self.dataset_id = dataset_id

    def get_logger(self, level):
        log = logging.getLogger()
        log.setLevel(level)
        log.addHandler(logging.StreamHandler(sys.stdout))

        return log

    def write_to_bq(self, table_name, file_name, append=False):
        """
        Write file at file_name to table in BQ.
        """
        self.log.info(f"Writing {table_name} to BQ from file {file_name}")
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_name.lower().replace("-","_"))

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'NEWLINE_DELIMITED_JSON'
        job_config.ignore_unknown_values = True

        current_tables = [x.table_id for x in self.bq_client.list_tables(dataset_ref)]

        if table_name in current_tables:
            table = self.bq_client.get_table(table_ref)
            job_config.schema = table.schema
        else:
            job_config.autodetect = True
        
        if append == False:
            job_config.write_disposition = "WRITE_TRUNCATE"
        else:
            job_config.write_disposition = "WRITE_APPEND"


        with open(file_name, 'rb') as source_file:
            job = self.bq_client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config)  # API request
            
        try:
            job.result()  # Waits for table load to complete.
        except: 
            self.log.info(job.errors)
            job.result()

    def write_to_json(self, file_name, jayson, mode='w'):
        """
        Provide a table_name and a dict object and I will write it in line-delimited
        JSON.
        """
        with open(file_name, mode) as f:
            for line in jayson:
                f.writelines(json.dumps(line) + '\n')

    def call_api(self, url, method='GET', headers=None, params=None, data=None):
        """
        Provide an endpoint and a method ('GET', 'POST', etc.), along with other arguments.
        Headers and params must be in dict form. Returns JSON.
        """
        r = requests.request(method=method, url=url, headers=headers, params=params, data=None)
        
        self.log.info(f'Called endpoint {url} with result {r}')

        try:
            jayson = json.loads(r.text)
            return jayson
        except:
            self.log.info(f'ERROR! Text of response object: {r.text}')

    def date_range(self, start_datetime, end_datetime, chunk_size):
        """
        Takes start and end datetimes and chunks the period into n-days
        size chunks.
        """
        self.log.info(f'Chunking period {start_datetime} to {end_datetime} into chunks of {chunk_size} days.')
        for n in range(int ((end_datetime - start_datetime).days)):
            if n/chunk_size == int(n/chunk_size):
                start = start_datetime + datetime.timedelta(n)
                end = start_datetime + datetime.timedelta(n+chunk_size)
                
                # if we reach the end_datetime, return that instead of end
                if end < end_datetime:
                    yield (start, end)
                else:
                    yield (start, end_datetime)

    def find_last_entry(self, table_name, date_column):
        query = f"SELECT MAX({date_column}) FROM `{self.dataset_id}.{table_name}`"
        query_job = self.bq_client.query(query)  # API request
        rows = query_job.result()
        latest_time = [x[0] for x in rows][0]

        return latest_time