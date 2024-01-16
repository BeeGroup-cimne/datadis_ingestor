import os
import subprocess
import sys
import tarfile
import time
import logging
import settings
from utils import utils
from pyhive import hive


program = logging.getLogger(__name__)
program.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
program.addHandler(handler)

MODULE_NAME = "join_databases"
HDFS_PATH = f'/user/ubuntu/tmp/{MODULE_NAME}'
MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/jobs/datadis:latest'
RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'

TABLES_TO_JOIN = [
    ['development_raw_data2:raw_Datadis_ts_EnergyConsumptionGridElectricity_PT15M_icaen',
     'development_raw_data2:raw_datadis_ts_EnergyConsumptionGridElectricity_PT15M_icaen'],
    ['development_raw_data2:raw_Datadis_ts_EnergyConsumptionGridElectricity_PT1H_icaen',
     'development_raw_data2:raw_datadis_ts_EnergyConsumptionGridElectricity_PT1H_icaen']
]


def create_hive_table_from_hbase(hive_table, hbase_table, key_struct, value_struct):
    key_line = ", ".join(f"{v}:{t}" for v, t in key_struct)
    value_line = ", ".join(f"{v} {t}" for v, t, _ in value_struct)
    values_hbase = ", ".join(f"{n}:{v}" for v, t, n in value_struct)
    return f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table} (key struct<{key_line}>, {value_line}) 
        ROW FORMAT DELIMITED COLLECTION ITEMS TERMINATED BY '~' 
        STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
        WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, {values_hbase}') 
        TBLPROPERTIES ('hbase.table.name' = '{hbase_table}')
    """


def create_input_file_from_tables(input_file_name, hbase_tables):
    tables_query = [f"""(SELECT key.hash, key.start_time, consumptionKWh, datetime, obtainMethod FROM {t})""" for t in hbase_tables]
    union_part = " UNION ALL ".join(tables_query)
    input_query = \
        f"""INSERT OVERWRITE DIRECTORY '{input_file_name}' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' {union_part}"""
    return input_query

print("prepare mr job", end=": ")
archives = []
files = []
s = time.time()
program.info("Create mapreduce archives")
tar = tarfile.open("mapreduce.tgz", 'w|gz')
for x in os.listdir("mapreduce"):
    tar.add(f"mapreduce/{x}", arcname=x)
tar.close()
tar = tarfile.open("utils.tgz", 'w|gz')
for x in os.listdir("utils"):
    tar.add(f"utils/{x}", arcname=x)
tar.close()
archives.append("mapreduce.tgz#mapreduce")
archives.append("utils.tgz#utils")
files.append("amapper.py#amapper.py")
files.append("settings.py#settings.py")
files.append(f"{settings.CONF_FILE}#config.json")
program.info(f"Took : {time.time() - s}")


hive_key = [("hash", "string"), ("start_time", "bigint")]
hive_values = [("consumptionKWh", "float", "info"), ("obtainMethod", "string", "info"), ("datetime", "string", "info")]

for tl in TABLES_TO_JOIN:

    subprocess.call(["hdfs", "dfs", "-rm", "-r", f'{HDFS_PATH}'],
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    subprocess.call(["hdfs", "dfs", "-mkdir", "-p", f'{HDFS_PATH}'],
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    input_file_name = f"{HDFS_PATH}/input"
    output_file_name = f"{HDFS_PATH}/output"
    config = utils.config.read_config()
    cursor = hive.connect(config['hive']['host']).cursor()
    cursor.execute(f"use {config['hive']['db']}", async_=False)
    hitables = []
    data_type, freq = None, None
    for index, t in enumerate(tl):
        _, _, _, data_type, freq, _ = t.split(":")[1].split("_")
        hi_table = f"{t.split(':')[1]}_{index}"
        hitables.append(hi_table)
        query = create_hive_table_from_hbase(hi_table, t, hive_key, hive_values)
        print(query)
        cursor.execute(query, async_=False)
    query = create_input_file_from_tables(input_file_name, hitables)
    print(query)
    cursor.execute(query, async_=False)

    with open("params.txt", "w") as param:
        param.write(f"{config['hbase']['raw_data'].format(data_type=data_type, freq=freq)}\n")
    files.append(f"params.txt#params.txt")

    s = time.time()
    program.info("Launching mrjob")
    system_call = ['mapred', 'streaming',
                   f'-Dmapreduce.map.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.reduce.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.job.name={MODULE_NAME}',
                   f'-Dmapred.reduce.tasks=10',
                   "-archives", ",".join(archives),
                   "-files", ",".join(files),
                   '-mapper', 'amapper.py',
                   '-input', input_file_name,
                   '-output', output_file_name]
    program.debug(" ".join(system_call))
    subprocess.call(system_call,
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    program.info(f"Took : {time.time() - s}")





