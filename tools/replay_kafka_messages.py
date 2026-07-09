#!/usr/bin/env python3
"""
Replays a JSONL file of pre-built Kafka messages (as produced by
tools/datadis_downloader.py's --db / --timeseries-kafka output) onto the real
Kafka cluster, using the exact same config.json / beelib.beekafka connection
launcher.py and DatadisGatherer.py use in production.

Each line in the file must be a JSON object: {"topic": ..., "key": ..., "value": ...}.
"value" is sent as-is (it's already the full envelope DatadisGatherer builds), so this
does NOT go through beelib.beekafka.send_to_kafka's own envelope-building - it publishes
the pre-built value directly, matching the file's contents byte-for-byte.

    python3 tools/replay_kafka_messages.py testing2/kafka_timeseries.jsonl
    python3 tools/replay_kafka_messages.py testing2/kafka_timeseries.jsonl --limit 10 --dry-run
"""
import argparse
import json
import logging
import os
import sys

import dotenv
import beelib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("replay_kafka_messages")


def replay(path, limit=None, dry_run=False):
    dotenv.load_dotenv()
    config = beelib.beeconfig.read_config()

    producer = None
    if not dry_run:
        producer = beelib.beekafka.create_kafka_producer(config['kafka']['connection'], encoding="JSON")

    sent, errors = 0, 0
    with open(path) as f:
        for i, line in enumerate(f, start=1):
            if limit is not None and sent >= limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
                topic, key, value = message['topic'], message.get('key'), message['value']
            except Exception as e:
                errors += 1
                logger.error(f"line {i}: malformed message, skipping: {e}")
                continue

            if dry_run:
                logger.info(f"line {i}: [dry-run] would send to topic={topic!r} key={key!r}")
            else:
                try:
                    producer.send(topic, key=key.encode('utf-8') if key else None, value=value)
                except Exception as e:
                    errors += 1
                    logger.error(f"line {i}: failed to send to topic={topic!r} key={key!r}: {e}")
                    continue

            sent += 1
            if sent % 500 == 0:
                logger.info(f"...sent {sent} messages so far")

    if producer:
        producer.flush()
        producer.close()

    logger.info(f"Done. Sent {sent} messages, {errors} errors, from {path}")


def main():
    parser = argparse.ArgumentParser(description="Replay a JSONL file of Kafka messages onto the real cluster")
    parser.add_argument("file", help="Path to the JSONL file (one {topic,key,value} message per line)")
    parser.add_argument("--limit", type=int, default=None, help="Only send the first N messages (smoke test)")
    parser.add_argument("--dry-run", action="store_true",
                         help="Parse and log what would be sent without connecting to Kafka")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    replay(args.file, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()