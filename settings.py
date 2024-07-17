import os
from dotenv import load_dotenv
load_dotenv()
CONF_FILE = os.environ.get("CONF_FILE", "config.json")
TOPIC_STATIC = "datadis.harm"
TOPIC_STATIC_PARTITIONS = 10
TOPIC_TS = "datadis.hbase"
TOPIC_TS_PARTITIONS = 10
TS_BUCKETS = 10000000
BUCKETS = 20
