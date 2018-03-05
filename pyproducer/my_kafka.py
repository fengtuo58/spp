import json
import logging
from pykafka import KafkaClient
from os import listdir
from os.path import isfile, join

from pykafka.common import CompressionType

# logging.basicConfig(level=logging.INFO)


def send(path, params):
    """
     e.g params = dict(server_url="localhost:9092", use_rdkafka=False, topic=b'test')
    """
    # set default values if not set before
    params.setdefault('use_rdkafka', False)
    params.setdefault('topic', b'test')
    params.setdefault('batch', 500)

    logging.info('Setup connection to {}'.format(params["server_url"]))
    client = KafkaClient(hosts=params['server_url'])
    topic = client.topics[params['topic']]
    producer = topic.get_producer(use_rdkafka=params['use_rdkafka'], compression=CompressionType.GZIP)

    logging.info('Load json from {}'.format(path))
    filelist = [f for f in listdir(path) if isfile(join(path, f))]

    def _send(index):
        logging.info('read json files batch from {} to {} ...'.format(index, index + params['batch']))
        json_msgs = []
        for f in filelist[index:index + params['batch']]:
            filename = '{path}/{f}'.format(path=path, f=f)
            with open(filename) as json_data:
                try:
                    json_msgs.append(json.load(json_data))
                except Exception as e:
                    logging.error('Cannot parse file {}, skip'.format(filename))
                    logging.exception(e)

        logging.info('send data ...')
        for msg_payload in json_msgs:
            producer.produce(bytes(json.dumps(msg_payload), 'utf-8'))

    n_files = len(filelist)
    current_index = 0
    while current_index < n_files:
        _send(current_index)
        current_index += params['batch']

    producer.stop()
