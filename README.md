# Popelines

This is a simple ETL tool for BigQuery, named for the author's surname. 

Popelines provides some basic functionality, such as writing to line-delimited JSON, writing to BigQuery, chunking dates, and other tools that are often needed when writing an ETL. It's sparse now, but I plan to expand it to include other Google Cloud functionalities.

Install
-------
To install popelines:
```bash
$ pip install instagram-scraper
```

Usage
-----

To get started:
```python
import popelines

pope = popelines.popeline(dataset_id='', service_key_file_loc=None, directory='.', verbose=False)
```

Providing a `dataset_id` is required. Everything else is optional - a service key will be inferred from your GOOGLE_ACCOUNT_CREDENTIALS env variable if not provided in `service_key_file_loc`, and `directory` defaults to the current directory if not provided. 

Popelines does some big handy things like you might expect:
```python
# write a dict to line-delimited JSON, perfect for uploading to BQ
pope.write_to_json(file_name=file_name, jayson=your_dict, mode='w')

# then you can turn around and upload that line-delimtited JSON...
pope.write_to_bq(table_name=table_name, file_name=file_name, append=True, 
    ignore_unknown_values=False, bq_schema_autodetect=False)

# or you can write it to GCS! leave bucket_name=None and popelines
# will try to upload to a bucket with the dataset_id you gave when you
# first initialized your pope object!
pope.write_to_gcs(gcs_path='folder/file.py', file_name='file.py', bucket_name=None)

# you can even call your API endpoints! This method returns a dict of data.
data = pope.call_api(url=url, method='GET', headers=None, params=None, data=None)
```

Popelines also does small handy things:
```python
# get a logger at your chosen verbosity and use it to log things
log = pope.log
log.info('Does the code get to this point?')

# chunk a date range into chunks n-days large
start_datetime = datetime.datetime(2018, 3, 1)
end_datetime = datetime.datetime(2018, 9, 1)
for day in pope.chunk_date_range(start_datetime=start_datetime, end_datetime=end_datetime, chunk_size=1):
    print(f"Wow, I'm glad it's not still {day}!!")

# find the last entry in a table - basically, query for the MAX() of a column
latest_day = pope.find_last_entry(table_name='my_table', date_column='day')
```

Finally, Popelines even does weird experimental things:
```python
# messed up JSON keys? fix_json_keys takes your dict obj and a callback
# function and applies the callback to each key recursively!
my_good_json = pope.fix_json_keys(obj=my_bad_json, callback=key_fixing_function)

# if your JSON values are messed up, have no fear! There is a similar 
# function for that!
my_good_json = pope.fix_json_values(obj=my_bad_json, callback=value_fixing_function)
```
*Note that `key_fixing_function` should take one argument (the key) while `value_fixing_function` must handle both a value and a key as arguments.*