from kafka import KafkaProducer
from json import dumps

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         compression_type='gzip')
topic_name = 'ark-topic-complexjson.pydict'

def kfk_insert(topic, insert_count):
    for _ in range(insert_count):
        smd_row_data = {"before":{"CTLID":2324,"OKO":"Y"},"after":{"CTLID":1554,"OKO":"Ne"}}
        future = producer.send(topic=topic, value=smd_row_data)
        message_metadata = future.get(timeout=10)
        print(f"""[+] message_metadata: {message_metadata.topic}, partition: {message_metadata.partition}, offset: {message_metadata.offset}""")

    print(f'{topic = } finish insert')

kfk_insert(topic_name, 2)

producer.close()