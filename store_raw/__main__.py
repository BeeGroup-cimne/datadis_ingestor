import faust
import time
import pandas as pd
import utils
import settings

config = utils.config.read_config()
app = faust.App('store_raw', topic_disable_leader=True, broker=f"kafka://{config['kafka']['host']}:{config['kafka']['port']}")
messages = app.topic(settings.TOPIC_TS, internal=True, partitions=settings.TOPIC_TS_PARTITIONS, value_serializer='pickle')


@app.agent(messages)
async def store_raw(records):
    async for record in records:
        if record['collection_type'] == "timeseries":
            start = time.time()
            df = pd.DataFrame.from_records(record['data'])
            #TODO; freq no esta be
            hbase_table = config['hbase']['raw_data'].format(data_type=record['property'], freq=record['freq'])
            row_keys = record['row_keys']
            try:
                utils.hbase.save_to_hbase(df.to_dict(orient="records"), hbase_table, config['hbase']['connection'],
                                          [("info", "all")], row_keys)
            except Exception as e:
                print("hoa")
                print(e)
                print(df.to_dict(orient="records"))
            print(f"processing time {time.time()-start}")

if __name__ == "__main__":
    app.main()

