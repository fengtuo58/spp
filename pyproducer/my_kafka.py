import json
import logging
from pykafka import KafkaClient
from pykafka.common import CompressionType
from os import listdir
from os.path import isfile, join

logging.basicConfig(level=logging.ERROR)


def send(path, params):
    """
     e.g params = dict(server_url="localhost:9092", use_rdkafka=False, topics=b'test')
    """
    logging.info('Setup connection to {}'.format(params["server_url"]))
    client = KafkaClient(hosts=params['server_url'])
    topic = client.topics[params['topic']]
    producer = topic.get_producer(use_rdkafka=params['use_rdkafka'], compression=CompressionType.GZIP)

    logging.info('Load json from {}'.format(path))
    filelist = [f for f in listdir(path) if isfile(join(path, f))]
    json_msgs = []
    for f in filelist:
        filename = '{path}/{f}'.format(path=path, f=f)
        logging.info('load json from {}'.format(filename))
        with open(filename) as json_data:
            try:
                json_msgs.append(json.load(json_data))
            except Exception as e:
                logging.error('Cannot parse file, skip')
                logging.exception(e)

    logging.info('send data ...')
    for msg_payload in json_msgs:
        producer.produce(bytes(json.dumps(msg_payload), 'utf-8'))
    producer.stop()
