import json
import os
import time
from random import randint

import my_kafka


def _generate_test_data():
    n_messages = 1000000

    msg_payload = {"rpcookie": "45555", "data": [3333, 3344]}
    msg_len = len(bytes(json.dumps(msg_payload), 'utf-8'))

    def _generate():
        msg = {'rpcookie': str(randint(0, 99999)).zfill(5), 'data': []}
        for d in range(2):
            msg['data'].append(randint(1000, 9999))

    path = 'json'
    if not os.path.exists(path):
        os.mkdir(path)
        for f in range(n_messages):
            filename = '{path}/{f}.json'.format(path=path, f=f)
            with open(filename, 'w') as outfile:
                json.dump(_generate(), outfile)
    else:
        n_messages = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
    print('Using:', json.dumps(msg_payload), ' length = ', msg_len, ' count = ', n_messages)
    return n_messages, msg_len


def calculate_thoughput(timing, n_messages=10000, msg_size=0):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024 * 1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


def pykafka_producer_performance():
    produce_start = time.time()
    my_kafka.send('json', dict(server_url='127.0.0.1:9092', use_rdkafka=False, topic=b'test', batch=10**3))
    return time.time() - produce_start


producer_timings = {}
consumer_timings = {}

n_messages, msg_size = _generate_test_data()
producer_timings['pykafka_producer'] = pykafka_producer_performance()
calculate_thoughput(producer_timings['pykafka_producer'], n_messages, msg_size)
