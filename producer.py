from time import sleep
from json import dumps
import urllib
import json
from kafka import KafkaProducer
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


for submission in range(1000):
    url = "https://codeforces.com/api/user.status?handle=toxic_hack&from="+str(submission + 1)+"&count=1"
    response = requests.get(url = url)
    # print(response.read())
    data = response.json()
    data = json.dumps(data)
    print(data)
    producer.send('numtest', value=data)
    sleep(5)
