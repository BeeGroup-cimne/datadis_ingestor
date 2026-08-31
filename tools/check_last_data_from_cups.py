from beelib.beehbase import get_hbase_data_batch

hbase_conf = {'host': 'localhost', 'port': 9090}
cups = "ES0031405170512001NE0F"
target_table = "datadis:raw_datadis_ts_EnergyConsumptionGridElectricity_PT1H"

# 1. Define reverse boundaries
row_start = f"{cups}~9999999999999"
row_stop = f"{cups}~"

# 2. Execute the reverse batch generator
generator = get_hbase_data_batch(
    hbase_conf=hbase_conf,
    hbase_table=target_table,
    row_start=row_start,
    row_stop=row_stop,
    reverse=True,
    limit=1
)

# 3. Extract the single record
latest_data = None
for batch in generator:
    if batch:
        latest_data = batch[0]
        break

# 4. Process the result
if latest_data:
    row_key, columns = latest_data
    print(f"✅ Success! Latest Row Key: {row_key.decode()}")

    # HappyBase returns columns as byte strings, so they require decoding
    decoded_columns = {k.decode(): v.decode() for k, v in columns.items()}
    print(f"Values: {decoded_columns}")
else:
    print(f"No data found for CUPS: {cups}")