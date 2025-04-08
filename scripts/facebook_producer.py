from facebook_scraper import get_posts
from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pages = ['OrangeSenegal', 'FreeSénégal', 'ExpressoSénégal']

while True:
    for page in pages:
        for post in get_posts(page, pages=1):
            data = {
                'operator': page,
                'text': post['text'],
                'time': str(post['time']),
                'likes': post['likes'],
                'comments': post['comments'],
                'shares': post['shares']
            }
            print(f"Envoyé : {data['text'][:60]}...")
            producer.send('facebook_posts', data)
    time.sleep(300)  # scrape toutes les 5 min
