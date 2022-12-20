from kafka import KafkaConsumer
import json
from psycopg2 import connect
from datetime import datetime
import pytz


# create consumer
consumer = KafkaConsumer('TopicCurrency', bootstrap_servers=['kafka:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# init postges connection
conn = connect(
    database="postgres",
    user="postgres",
    host='postgres',
    port='5432',
    password="anypassword")
cursor = conn.cursor()



rates_mapping = {'EURUSD':'US Dollar', 'EURGBP':'Pound Sterling', 'USDEUR':'Euro'}
insert_query = "INSERT INTO public.currency(currency_id, rate, timestamp, currency_name) VALUES "
tz = pytz.timezone('Asia/Jakarta')
for msg in consumer:
    data = msg['rates']

    values = []
    for x in data.keys():
        currency_id = x
        rate = data[x]['rate']
        timestamp = datetime.fromtimestamp(data[x]['timestamp'], tz=tz).strftime('%Y-%m-%d %H:%M:%S')
        currency_name = rates_mapping[x]
        values.append(f"({currency_id}, {rate}, {timestamp}, {currency_name})") 

    values = ', '.join(values)
    query = insert_query + values

    cursor.execute(query)
    conn.commit()
