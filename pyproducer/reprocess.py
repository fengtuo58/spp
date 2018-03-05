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
input_topic = b'test'
output_topic = b'out'



def get_consumer(use_rdkafka=False):
    client = KafkaClient(hosts=bootstrap_servers)
    topic = client.topics[input_topic]
    consumer = topic.get_simple_consumer(use_rdkafka=use_rdkafka)
    return consumer

def get_producer(use_rdkafka=False):
    client = KafkaClient(hosts=bootstrap_servers)
    topic = client.topics[output_topic]
    producer = topic.get_producer(use_rdkafka=use_rdkafka)
    return producer


consumer = get_consumer()
producer = get_producer()
msg_consumed_count = 0

for message in consumer:
    msg = consumer.consume()
    if msg:
        msg_consumed_count += 1
        msg_string = msg.value.decode('utf8')
        try:
            msg_json = json.loads(msg_string) or {}
            msg_json["repro"] = "success"
            producer.produce(bytes(json.dumps(msg_json), 'utf-8'))
            logging.info('reprocessed {} messages'.format(msg_consumed_count))
        except json.decoder.JSONDecodeError as e:
            logging.info('Skip incorrect message {}: {}'.format(msg_string, e))
