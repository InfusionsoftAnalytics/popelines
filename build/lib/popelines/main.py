import json
from google.cloud import bigquery
from google.cloud import storage
import os
import logging
import requests
import sys
import datetime

class popeline:
    """
    popeline creates a data pipeline for Google's BigQuery. 
    """
    def __init__(self, dataset_id, service_key_file_loc=None, directory='.', verbose=False):

        # set up GCS and BQ clients - if no service_account_json provided, then pull
        # from environment variable
        if service_key_file_loc:
            self.bq_client = bigquery.Client.from_service_account_json(service_key_file_loc)
            self.gcs_client = storage.Client.from_service_account_json(service_key_file_loc)
        else:
            self.bq_client = bigquery.Client()
            self.gcs_client = storage.Client()

        # set up a logger
        self.log = self.get_logger(verbose)

        # get local directory
        self.directory = directory

        self.dataset_id = dataset_id

    def get_logger(self, verbose):
        """
        Does basically what you would expect. 
        """
        log_levels = [logging.INFO, logging.DEBUG]

        log = logging.getLogger()
        log.setLevel(log_levels[int(verbose)])
        
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_levels[int(verbose)])
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
        ch.setFormatter(formatter)
        log.addHandler(ch)

        return log

    def generate_bq_schema(self, file_name, schema_file_name=None):
        """
        Uses CLI tool bigquery_schema_generator to generate an API
        representation of a schema
        """
        if not schema_file_name:
            schema_file_name = f'{self.directory}/schema.json'
        os.system(f"generate-schema --keep_nulls < {file_name} > {schema_file_name}")

        schema = open(schema_file_name, 'r').read()

        return json.loads(schema)

    def merge_schemas(self, old_schm, new_schm):
        """
        Run through new_schm and add any fields not in old_schm
        to old_schm.
        """

        old_schm_cols = [x['name'] for x in old_schm]

        for col in new_schm:
            if type(col) == dict:
                if col['name'] not in old_schm_cols:
                    old_schm.append(col)
        
        for count, old_col in enumerate(old_schm):
            for meta in old_col:
                if type(old_col[meta]) == list:
                    if old_col['name'] in [pot_new_col['name'] for pot_new_col in new_schm]:
                        new_col = [pot_new_col for pot_new_col in new_schm if pot_new_col['name'] == old_col['name']][0]
                        if meta in new_col:
                            old_schm[count][meta] = self.merge_schemas(old_col[meta], new_col[meta])
                    
        return old_schm

    def write_to_bq(self, table_name, file_name, append=True, ignore_unknown_values=False, bq_schema_autodetect=False):
        """
        Write file at file_name to table in BQ.
        """
        table_name = table_name.lower().replace("-","_")
        self.log.info(f"Writing {table_name} to BQ from file {file_name}")
        dataset_ref = self.bq_client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_name)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'NEWLINE_DELIMITED_JSON'

        if bq_schema_autodetect == False:
            # prepare for schema manipulation
            current_tables = [x.table_id for x in self.bq_client.list_tables(dataset_ref)]
            new_schm = self.generate_bq_schema(file_name)

            # if table exists, edit schema. otherwise, use new_schm
            if table_name in current_tables:	
                table = self.bq_client.get_table(table_ref)	
                new_schm = self.merge_schemas(table.to_api_repr()['schema']['fields'], new_schm)

            # move new_schm into job_config through the api_repr options
            api_repr = job_config.to_api_repr()
            api_repr['load']['schema'] = {'fields': new_schm}
            job_config = job_config.from_api_repr(api_repr)
        else:
            job_config.autodetect = True
        
        # handle write options
        if append == False:
            job_config.write_disposition = "WRITE_TRUNCATE"
        else:
            job_config.write_disposition = "WRITE_APPEND"
            job_config.schema_update_options = ['ALLOW_FIELD_ADDITION']

        if ignore_unknown_values:
            job_config.ignore_unknown_values = True

        # send to BQ
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

    def write_to_gcs(self, gcs_path, file_name, bucket_name=None):
        self.log.info('Uploading to GCS...')

        if bucket_name:
            bucket = self.gcs_client.get_bucket(bucket_name)
        else:
            bucket = self.gcs_client.get_bucket(self.dataset_id)

        blob = storage.Blob(gcs_path, bucket)
        blob.upload_from_string(open(file_name, 'r').read())

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
        r = requests.request(method=method, url=url, headers=headers, params=params, data=data)
        
        self.log.debug(f'Called endpoint {url} with result {r}')

        try:
            jayson = json.loads(r.text)
            return jayson
        except:
            self.log.info(f'ERROR! Text of response object: {r.text}')

    def chunk_date_range(self, start_datetime, end_datetime, chunk_size):
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
        """
        Returns maximum value from date_column in table_name.
        """
        query = f"SELECT MAX({date_column}) FROM `{self.dataset_id}.{table_name}`"
        query_job = self.bq_client.query(query)  # API request
        rows = query_job.result()
        latest_time = [x[0] for x in rows][0]

        return latest_time

    def bq_query(self, query):
        """
        Returns maximum value from date_column in table_name.
        """
        query_job = self.bq_client.query(query)  # API request
        rows = [x for x in query_job.result()]
        
        return rows

    def fix_json_keys(self, obj, callback):
        """
        Runs all keys in a JSON object (dict or list) through 
        the given callback function.
        """
        if type(obj) == list:
            newlist = []
            for item in obj:
                newlist.append(self.fix_json_keys(item, callback))
            return newlist
        elif type(obj) == dict:
            newdict = {}
            for item in list(obj):
                if type(obj[item]) == list or type(obj[item]) == dict:
                    newdict[callback(item)] = self.fix_json_keys(obj[item], callback)
                else:
                    newdict[callback(item)] = obj[item]
            return newdict

    def fix_json_values(self, obj, callback):
        """
        Runs all values in a JSON object (dict or list) through 
        the given callback function. Callback should be passed two
        arguments, the value of the key:value pair (`obj[item]` below)
        and the key (`item` below).
        """
        if type(obj) == list:
            newlist = []
            for item in obj:
                newlist.append(self.fix_json_values(item, callback))
            return newlist
        elif type(obj) == dict:
            newdict = {}
            for item in list(obj):
                if type(obj[item]) == list or type(obj[item]) == dict:
                    newdict[item] = self.fix_json_values(callback(obj[item], item), callback)
                else:
                    newdict[item] = callback(obj[item], item)
            return newdict
        else:
            return obj