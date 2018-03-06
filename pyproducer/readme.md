### Set up environment

If you go on local machine you will need Java in order to set up Kafka broker


### Using my_kafka.py  module

Place my_kafka.py into dir or extend syspath to the module, than use it as usual module

```import mykafka
my_kafka.send('path_to_files', dict(server_url='192.168.1.117:9092', use_rdkafka=False, topic=b'test', batch=100))
```
Replace 'path_to_files' to string with path where your file are reside

Dict values should be replaced acording to your Kafka environment except batch
```
batch = 100 
```
This parameter is used to set number of files to be read in memory before producing messages



### Using reprocess.py  module


```import reprocess 
process({'in_server_url': 'localhost:9092', 'out_server_url': 'localhost:9092', 'input_topic', b'test', 'output_topic', b'out'})
```

For testing purpose module can be runnede as python script
```
$python reprocess.py
```
