import contextlib
import faust
from plugins.infraestructures.harmonizer_infra import harmonize_supplies, harmonize_timeseries, end_process
import settings
import beelib
import os

config = beelib.beeconfig.read_config()
app = faust.App('datadis.icat.harm', topic_disable_leader=True, broker=f"kafka://{config['kafka']['host']}:{config['kafka']['port']}")

static = app.topic(settings.TOPIC_STATIC, internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                   value_serializer='json')
supplies_table = app.Table('datadis.supplies_table_cache', partitions=settings.TOPIC_STATIC_PARTITIONS)
harmonize_supply = app.topic('datadis.icat.harmonize_supplies',  internal=True, partitions=settings.TOPIC_STATIC_PARTITIONS,
                             value_serializer='json')


@app.agent(static)
async def join_supplies(records):
    async for record in records:
        if record['kwargs']['collection_type'] == "FINAL_MESSAGE":
            print("FINAL_MESSAGE received")
            await process_table.cast(value=record)
            continue
        if "icat" not in record['kwargs']['dblist']:
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
            print(f"processing {len(messages)} supplies")
            with open(os.devnull, 'w') as devnull:
                with contextlib.redirect_stdout(devnull):
                    with contextlib.redirect_stderr(devnull):
                        harmonize_supplies(messages)
        if final:
            print("processing final event")
            for k in supplies_table.keys():
                print(k)
                del supplies_table[k]
            end_process()

if __name__ == '__main__':
    app.main()
