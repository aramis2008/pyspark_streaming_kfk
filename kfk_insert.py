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
topic_name_2 = 'ark-topic-2'

k = 0
while k < 20:
    k += 1
    random_data_1 = getRandomValue()
    random_data_2 = getRandomValue()
    print(f"step: {k}")
    # print(random_data_1)
    try:
        future_1 = producer.send(topic=topic_name_1, value=random_data_1)
        message_metadata_1 = future_1.get(timeout=10)

        print(
            f"""[+] message_metadata: {message_metadata_1.topic}, partition: {message_metadata_1.partition}, offset: {message_metadata_1.offset}""")

        future_2 = producer.send(topic=topic_name_2, value=random_data_2)
        message_metadata_2 = future_2.get(timeout=10)

        print(
            f"""[+] message_metadata: {message_metadata_2.topic}, partition: {message_metadata_2.partition}, offset: {message_metadata_2.offset}""")
    except Exception as e:
        print(f">>> Error: {e}")
    finally:
        producer.flush()
    sleep(2)

producer.close()