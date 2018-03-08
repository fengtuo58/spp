import json
import logging
from pykafka import KafkaClient
from pykafka.common import CompressionType

logging.basicConfig(level=logging.INFO)


def get_consumer(params):
    client = KafkaClient(hosts=params['in_server_url'])
    topic = client.topics[params['input_topic']]
    consumer = topic.get_simple_consumer(use_rdkafka=params['use_rdkafka'])
    return consumer


def get_producer(params):
    client = KafkaClient(hosts=params['out_server_url'])
    topic = client.topics[params['output_topic']]
    producer = topic.get_producer(use_rdkafka=params['use_rdkafka'], compression=CompressionType.GZIP)
    return producer


def process(params):
    """
        e.g params = dict(in_server_url="localhost:9092", out_server_url="localhost:9092",
         use_rdkafka=False, input_topic=b'test', output_topic='out')
    """
    # set default values if not set before
    params.setdefault('input_topic', b'test')
    params.setdefault('output_topic', b'out')
    params.setdefault('use_rdkafka', False)

    consumer = get_consumer(params)
    producer = get_producer(params)
    msg_consumed_count = 0

    for msg in consumer:
        if msg:
            msg_consumed_count += 1
            msg_string = msg.value.decode('utf8')
            try:
                msg_json = json.loads(msg_string)
                if isinstance(msg_json, dict):
                    msg_json["repro"] = "success"
                    producer.produce(bytes(json.dumps(msg_json), 'utf-8'))
                    logging.info('reprocessed {} messages'.format(msg_consumed_count))
            except json.decoder.JSONDecodeError as e:
                logging.info('Skip incorrect message {}: {}'.format(msg_string, e))


if __name__ == "__main__":
    # for testing purpose
    process({'in_server_url': 'localhost:9092', 'out_server_url': 'localhost:9092'})
