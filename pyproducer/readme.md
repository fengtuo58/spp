### Set up environment

If you go on local machine you will need Java in order to set up Kafka broker




place my_kafka.py into dir, than use it as usual module
```import mykafka
my_kafka.send('json', dict(server_url='192.168.1.117:9092', use_rdkafka=False, topic=b'test'))
```
where json is path to files dir
