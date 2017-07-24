import boto3
import json
from datetime import datetime
import time
import json
import ast
import pandas as pd
from pandas.io.json import json_normalize
from json import dumps

my_stream_name = 'big-data-analytics-desafio'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

my_shard_id = 'shardId-000000000000'

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                   ShardId=my_shard_id,
                                                   ShardIteratorType='AT_TIMESTAMP',
                                                   Timestamp=datetime(2015, 1, 1)
                                                   )

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'])

    print (record_response)
    # wait for 5 seconds
    time.sleep(2)