# -*- coding: utf-8 -*-
"""
Kafka Consumer Sink to HDFS Data Lake
Topic: spotify-stream -> Sink: /datalake/spotify/streaming/ (Parquet Snappy)
Author: Nhom 9 - Big Data Final Project
"""
import json
import time
import os
import argparse
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def run_consumer(bootstrap_servers='localhost:9092', topic='spotify-stream', batch_size=5000, sink_dir='./output/streaming'):
    print(f"[*] Starting Kafka Consumer Sink for topic '{topic}'...")
    print(f"[*] Bootstrap servers: {bootstrap_servers}")
    print(f"[*] Micro-batch size: {batch_size} events")
    
    os.makedirs(sink_dir, exist_ok=True)
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(','),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='hdfs-sink-group'
    )
    
    buffer = []
    batch_counter = 0
    print("[*] Consumer connected successfully. Listening for streaming events...")
    
    try:
        for msg in consumer:
            buffer.append(msg.value)
            if len(buffer) >= batch_size:
                batch_counter += 1
                df_batch = pd.DataFrame(buffer)
                table = pa.Table.from_pandas(df_batch)
                out_file = os.path.join(sink_dir, f"part-{batch_counter:05d}-kafka-sink.parquet")
                pq.write_table(table, out_file, compression='SNAPPY')
                print(f"[SUCCESS] Flushed micro-batch #{batch_counter} ({len(buffer)} events) to: {out_file}")
                buffer.clear()
    except KeyboardInterrupt:
        if buffer:
            batch_counter += 1
            df_batch = pd.DataFrame(buffer)
            table = pa.Table.from_pandas(df_batch)
            out_file = os.path.join(sink_dir, f"part-{batch_counter:05d}-kafka-sink.parquet")
            pq.write_table(table, out_file, compression='SNAPPY')
            print(f"[FLUSH] Saved remaining {len(buffer)} events to: {out_file}")
        print("[INFO] Kafka Consumer stopped cleanly.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka Consumer Sink to HDFS')
    parser.add_argument('--servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='spotify-stream', help='Kafka topic name')
    parser.add_argument('--batch-size', type=int, default=5000, help='Micro-batch size')
    parser.add_argument('--sink-dir', default='./output/streaming', help='Local or HDFS sink output directory')
    args = parser.parse_args()
    
    run_consumer(args.servers, args.topic, args.batch_size, args.sink_dir)
