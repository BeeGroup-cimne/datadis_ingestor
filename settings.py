import os

CONF_FILE = os.environ.get("CONF_FILE", "config.json")
TOPIC_STATIC = "datadis_static"
TOPIC_STATIC_PARTITIONS = 10
TOPIC_TS = "datadis_ts"
TOPIC_TS_PARTITIONS = 10
TS_BUCKETS = 10000000
BUCKETS = 20
