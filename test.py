topic_name_1 = 'ark-topic-1'
topic_name_2 = 'ark-topic-2'
topic_name_3 = 'ark-topic-3'

startingOffsets = f"""{{"{topic_name_1}":{{"0":370}},"{topic_name_2}":{{"0":220}},"{topic_name_3}":{{"0":1550}} }}"""
print(startingOffsets)