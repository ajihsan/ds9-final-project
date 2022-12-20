from kafka import KafkaProducer
import requests
import json


url = "https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR"
data = requests.get(url).json()

producer = KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=lambda m: json.dumps(m).encode('utf-8'))

producer.send('TopicCurrency', data)