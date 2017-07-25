#coding: utf-8
import os, sys
import boto3
from datetime import datetime
import time
import json
import sqlite3

my_stream_name = 'big-data-analytics-desafio'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

my_shard_id = 'shardId-000000000000'

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                   ShardId=my_shard_id,
                                                   ShardIteratorType='TRIM_HORIZON'
                                                   #Timestamp=datetime(2015, 1, 1)
                                                   )

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'])

    # Connectando no banco de dados local
    conn = sqlite3.connect('desafio-luizalabs.db')

    # definindo um cursor
    c = conn.cursor()

    for o in record_response["Records"]:
        data = json.loads(o["Data"])

        # Inserindo dados necessarios para alinse, retirado da base de origem Kinesis
        c.execute('INSERT INTO analysis_email VALUES (?,?,?)', (data['event_id'], data['event_type'], data['datetime']))
        # Commit do insert
        conn.commit()