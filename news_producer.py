# news_producer.py
import requests, json, time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='ec2-13-219-225-137.compute-1.amazonaws.com:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

API_KEY = 'YOUR_NEWSAPI_KEY'
url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey=916bc86db1c944089064f0317d8cb1ee'

while True:
    res = requests.get(url).json()
    for article in res.get('articles', []):
        producer.send('news', {'title': article['title']})
    time.sleep(30)  
