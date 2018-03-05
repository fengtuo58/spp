import time
import json
import sys
from random import randint

msg_count = 10**6
msg_payload = {"rpcookie": "45555", "data": [3333, 3344]}
msg_len = len(bytes(json.dumps(msg_payload), 'utf-8'))
path = 'json'


def generate_json():
    msg = {}
    msg["rpcookie"] = str(randint(0,99999)).zfill(5)
    data = []
    for d in range(2):
        data.append(randint(1000,9999))
    msg["data"] = data
    return msg

msg_payload = generate_json()

print('Using template:',  json.dumps(msg_payload), ' length = ', msg_len)

file_data = []


for f in range(msg_count):
    filename = path + '/' + str(f) + '.json'
    with open(filename, 'w') as outfile:
        json.dump(generate_json(), outfile)
