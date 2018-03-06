import time
import json
import logging
from pykafka import KafkaClient
from pykafka.common import CompressionType

logging.basicConfig(level=logging.INFO)


msg_count = 10**6
msg_payload = {"rpcookie": "45555", "data": [3333, 3344]}
msg_len = len(bytes(json.dumps(msg_payload), 'utf-8'))

print('Calculations based on:',  json.dumps(msg_payload), ' length = ', msg_len)

bootstrap_servers = 'localhost:9092'
kafka_topic = b'test'

producer_timings = {}
consumer_timings = {}

def calculate_thoughput(timing, n_messages = msg_count, msg_size = msg_len):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

def pykafka_consumer_performance(use_rdkafka=False):
    # Setup client
    client = KafkaClient(hosts=bootstrap_servers)
    topic = client.topics[b'test']

    msg_consumed_count = 0
    
    consumer_start = time.time()
    # Consumer starts polling messages in background thread, need to start timer here
    consumer = topic.get_simple_consumer(use_rdkafka=use_rdkafka)

    while True:
        msg = consumer.consume()
        if msg:
            msg_consumed_count += 1

        if msg_consumed_count >= msg_count:
            break
                        
    consumer_timing = time.time() - consumer_start
    consumer.stop()    
    return consumer_timing

#_ = pykafka_consumer_performance(use_rdkafka=False)
consumer_timings['pykafka_consumer'] = pykafka_consumer_performance(use_rdkafka=False)
calculate_thoughput(consumer_timings['pykafka_consumer'])
