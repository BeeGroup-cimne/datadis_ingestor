import contextlib
import os
import logging
import beelib
import beelib.beekafka
from pythonjsonlogger import jsonlogger
from plugins.sime.harmonizer_sime import harmonize_supplies, harmonize_timeseries, end_process
import json

logger = logging.getLogger("datadis_harmonizer")
logger.propagate = False
logger.setLevel("INFO")

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)

if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(logHandler)


def cleanup_agent():
    logger.debug("Executing final event cleanup", extra={'phase': 'HARMONIZE_CLEANUP'})
    end_process()


def main():
    logger.info("Starting consumer")
    conf = beelib.beeconfig.read_config()
    consumer = beelib.beekafka.create_kafka_consumer(conf['kafka']['connection'], encoding="JSON",
                                                     group_id=conf['kafka']['consumer_group'])
    consumer.subscribe(topics=conf['kafka']['topics'])

    while True:
        raw_records = consumer.consumer.consume(100, 10)

        if len(raw_records) > 0:
            static_records = []
            ts_records = []

            for x in raw_records:
                if x.error() or x.value() is None:
                    continue

                try:
                    record_dict = json.loads(x.value().decode('utf-8'))

                    if x.topic() == conf['kafka']['static_topic']:
                        static_records.append(record_dict)
                    elif x.topic() == conf['kafka']['ts_topic']:
                        ts_records.append(record_dict)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode malformed Kafka message: {e}")
                    continue

            # 3. Process Static Records
            messages = [x['data'] for x in static_records if
                        x.get('kwargs', {}).get('collection_type') != "FINAL_MESSAGE"]
            final = [x for x in static_records if x.get('kwargs', {}).get('collection_type') == "FINAL_MESSAGE"]

            if messages:
                logger.debug("Processing supplies", extra={'phase': 'HARMONIZE', 'number': len(messages)})
                with open(os.devnull, 'w') as devnull:
                    with contextlib.redirect_stdout(devnull):
                        with contextlib.redirect_stderr(devnull):
                            harmonize_supplies(messages)
            if final:
                logger.debug("Dispatching final event to cleanup agent", extra={'phase': 'HARMONIZE_END'})
                cleanup_agent()

            for record in ts_records:
                kwargs = record.get('kwargs', {})
                data = record.get('data', [])

                if "sime" not in kwargs.get('dblist', []):
                    continue
                if kwargs.get('property') not in ["EnergyConsumptionGridElectricity"]:
                    continue

                harmonize_timeseries(data, kwargs.get('freq'), kwargs.get('property'))


if __name__ == '__main__':
    main()
