"""Create the 3 topics (idempotent). python -m scripts.create_topics"""
from confluent_kafka.admin import AdminClient, NewTopic
import config

a = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
topics = [config.TOPIC_STATION, config.TOPIC_SUPPLIER, config.TOPIC_TEAM]
for t, f in a.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]).items():
    try:
        f.result()
        print("created", t)
    except Exception as e:
        print(t, "->", "already exists" if "EXISTS" in str(e).upper() else e)
