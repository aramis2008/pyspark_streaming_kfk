from kafka import KafkaProducer
from json import dumps
from kafka.errors import KafkaError
from time import sleep
from numpy.random import choice, randint
import time
import datetime


def getRandomValue():
    new_dict = {}

    country_list = ['ISL', 'DEU', 'SWE', 'ESP', 'UGA', 'JPN']
    site_list = ['site1.ru', 'site2.com', 'site3.com', 'site4.es', 'site5.de', 'site6.jp',
                 'site7.ru', 'site8.com', 'site9.com', 'site10.es', 'site11.de', 'site12.jp']
    ts = time.time() + randint(-15, 0)
    event_time = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    new_dict['event_time'] = event_time
    new_dict['ad_id'] = randint(1, 5)
    new_dict['country'] = choice(country_list)
    new_dict['site'] = choice(site_list)
    new_dict['viewing_duration'] = randint(1, 15)

    return new_dict


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
topic_name_1 = 'ark-topic-1'
insert_count_1 = 5
topic_name_2 = 'ark-topic-2'
insert_count_2 = 3
topic_name_3 = 'ark-topic-3'
insert_count_3 = 20

def kfk_insert(topic, insert_count):
    for _ in range(insert_count):
        random_data = getRandomValue()
        producer.send(topic=topic, value=random_data)
        # future = producer.send(topic=topic, value=random_data)
        # message_metadata = future.get(timeout=10)
        # print(f"""[+] message_metadata: {message_metadata.topic}, partition: {message_metadata.partition}, offset: {message_metadata.offset}""")

    print(f'{topic = } finish insert')

k = 0
while k < 20:
    k += 1
    print(f"step: {k}")
    try:
        kfk_insert(topic_name_1, insert_count_1)
        kfk_insert(topic_name_2, insert_count_2)
        kfk_insert(topic_name_3, insert_count_3)
    except Exception as e:
        print(f">>> Error: {e}")
    finally:
        producer.flush()
    sleep(2)

producer.close()