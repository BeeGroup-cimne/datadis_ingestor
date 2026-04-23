import contextlib
import faust
from plugins.sime.harmonizer_sime import harmonize_supplies, harmonize_timeseries, end_process
import settings
import beelib
import os
import logging
from pythonjsonlogger import jsonlogger
from plugins.sime import SIMEImport

logger = logging.getLogger()
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

config = beelib.beeconfig.read_config()
app = faust.App('datadis.harm', topic_disable_leader=True,
                broker=f"kafka://{config['kafka']['host']}:{config['kafka']['port']}")

static = app.topic(settings.TOPIC_STATIC, internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                   value_serializer='json')
ts = app.topic(SIMEImport.topic, internal=True, partitions=settings.TOPIC_TS_PARTITIONS,
               value_serializer='json')
supplies_table = app.Table('datadis.sime.supplies_table_cache', partitions=settings.TOPIC_STATIC_PARTITIONS)
harmonize_supply = app.topic('datadis.sime.harmonize_supplies', internal=True,
                             partitions=settings.TOPIC_STATIC_PARTITIONS,
                             value_serializer='json')

cleanup_topic = app.topic('datadis.sime.cleanup', internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                          value_serializer='json')


@app.agent(static)
async def join_supplies(records):
    async for record in records:
        if record['kwargs']['collection_type'] == "FINAL_MESSAGE":
            logger.debug("FINAL_MESSAGE received", extra={'phase': 'HARMONIZE'})
            await process_table.cast(value=record)
            continue
        if "sime" not in record['kwargs']['dblist']:
            continue
        try:
            tmp = supplies_table.pop(record['data']['cups'])
            if not tmp:
                raise Exception()
            tmp.update(record['data'])
            record['data'] = tmp
            await process_table.cast(value=record)
        except:
            supplies_table[record['data']['cups']] = record['data']


@app.agent(cleanup_topic)
async def cleanup_agent(records):
    # This uses standard iteration, so Faust grants us an active event context!
    async for record in records:
        logger.debug("Executing final event cleanup", extra={'phase': 'HARMONIZE_CLEANUP'})

        # Safe copy of keys to prevent runtime iteration errors
        keys = list(supplies_table.keys())

        for k in keys:
            logger.debug("Supply cleanup", extra={'phase': 'HARMONIZE', 'supply': k})
            supplies_table[k] = {}  # This is now 100% safe

        end_process()


@app.agent(harmonize_supply)
async def process_table(records):
    async for record_batch in records.take(100, within=10):
        messages = [x['data'] for x in record_batch if x['kwargs']['collection_type'] != "FINAL_MESSAGE"]
        final = [x for x in record_batch if x['kwargs']['collection_type'] == "FINAL_MESSAGE"]
        if messages:
            logger.debug("Processing supplies", extra={'phase': 'HARMONIZE', 'number': len(messages)})
            with open(os.devnull, 'w') as devnull:
                with contextlib.redirect_stdout(devnull):
                    with contextlib.redirect_stderr(devnull):
                        harmonize_supplies(messages)
        if final:
            logger.debug("Dispatching final event to cleanup agent", extra={'phase': 'HARMONIZE_END'})
            await cleanup_agent.cast(value={"action": "cleanup"})


@app.agent(ts)
async def process_ts(records):
    async for record in records:
        if "sime" not in record['kwargs']['dblist']:
            continue
        if record['kwargs']['property'] not in ["EnergyConsumptionGridElectricity"]:
            continue
        harmonize_timeseries(record['data'], record['kwargs']['freq'], record['kwargs']['property'])


if __name__ == '__main__':
    app.main()
