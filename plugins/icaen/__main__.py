import contextlib
import faust
from plugins.icaen.harmonizer_sime import harmonize_supplies, harmonize_timeseries, end_process
import settings
import utils
import os
config = utils.config.read_config()
app = faust.App('datadis.harmonize', topic_disable_leader=True, broker=f"kafka://{config['kafka']['host']}:{config['kafka']['port']}")

static = app.topic(settings.TOPIC_STATIC, internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                   value_serializer='pickle')
ts = app.topic(settings.TOPIC_TS, internal=True, partitions=settings.TOPIC_TS_PARTITIONS,
               value_serializer='pickle')
supplies_table = app.Table('datadis.supplies_table_cache', partitions=settings.TOPIC_STATIC_PARTITIONS)
harmonize_supply = app.topic('datadis.harmonize_supplies',  internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                             value_serializer='pickle')


@app.agent(static)
async def join_supplies(records):
    async for record in records:
        if "sime" not in record['dblist']:
            continue
        if record['collection_type'] == "FINAL_MESSAGE":
            print("FINAL_MESSAGE received")
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
        messages = [x['data'] for x in record_batch if x['collection_type'] != "FINAL_MESSAGE"]
        final = [x for x in record_batch if x['collection_type'] == "FINAL_MESSAGE"]
        if messages:
            print(f"processing {len(messages)} supplies")
            with open(os.devnull, 'w') as devnull:
                with contextlib.redirect_stdout(devnull):
                    with contextlib.redirect_stderr(devnull):
                        harmonize_supplies(messages)
        if final:
            print("processing final event")
            for k in supplies_table.keys():
                print(k)
            end_process()


@app.agent(ts)
async def process_ts(records):
    async for record in records:
        if "sime" not in record['dblist']:
            continue
        if record['property'] not in ["EnergyConsumptionGridElectricity"]:
            continue
        harmonize_timeseries(record['data'], record['freq'], record['property'])


if __name__ == '__main__':
    app.main()
