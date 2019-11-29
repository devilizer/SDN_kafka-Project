from kafka import KafkaConsumer
from json import loads
from elasticsearch import Elasticsearch
from time import sleep
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('Awww it could not connect!')
    return _es

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group3',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

es = connect_elasticsearch()

for message in consumer:
    data = loads(str(message.value))
    res = es.index(index='codeforces',doc_type='user_data',id=1,body=data)
    # print("The recieved data is ", data)
    print("result is ", res)
    sleep(5)
