import contextlib
import faust
from plugins.icaen.harmonizer_sime import harmonize_supplies, harmonize_timeseries, end_process
import settings
import beelib
import os
import logging
from pythonjsonlogger import jsonlogger
from plugins.icaen import SIMEImport


logger = logging.getLogger()
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

config = beelib.beeconfig.read_config()
app = faust.App('datadis.harm', topic_disable_leader=True, broker=f"kafka://{config['kafka']['host']}:{config['kafka']['port']}")

static = app.topic(settings.TOPIC_STATIC, internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                   value_serializer='json')
ts = app.topic(SIMEImport.get_topic(), internal=True, partitions=settings.TOPIC_TS_PARTITIONS,
               value_serializer='json')
supplies_table = app.Table('datadis.supplies_table_cache', partitions=settings.TOPIC_STATIC_PARTITIONS)
harmonize_supply = app.topic('datadis.harmonize_supplies',  internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                             value_serializer='json')


@app.agent(static)
async def join_supplies(records):
    async for record in records:
        if "sime" not in record['kwargs']['dblist']:
            continue
        if record['kwargs']['collection_type'] == "FINAL_MESSAGE":
            logger.debug("FINAL_MESSAGE received", extra={'phase': 'HARMONIZE'})
            await process_table.cast(value=record)
            continue
        try:
            tmp = supplies_table.pop(record['data']['cups'])
            tmp.update(record['data'])
            record['data'] = tmp
            await process_table.cast(value=record)
        except:
            supplies_table[record['data']['cups']] = record['data']


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
            logger.debug("Processing final event", extra={'phase': 'HARMONIZE_END'})
            for k in supplies_table.keys():
                logger.debug("Supply", extra={'phase': 'HARMONIZE', 'supply': k})
            end_process()


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
