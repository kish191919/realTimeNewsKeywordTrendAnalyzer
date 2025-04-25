# news_producer.py
import os, requests, json, time
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('NEWS_API_KEY')
EC2_HOST = os.getenv('EC2')
url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={API_KEY}'

producer = KafkaProducer(
    bootstrap_servers=f'{EC2_HOST}:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    res = requests.get(url).json()
    for article in res.get('articles', []):
        producer.send('news', {'title': article['title']})
    time.sleep(30)
