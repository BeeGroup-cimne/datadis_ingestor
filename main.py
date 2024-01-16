import argparse
import datetime
import os
import pickle
import subprocess
import sys
import tarfile
import time
from base64 import b64encode

import pandas as pd
from beedis import datadis, ENDPOINTS
from kafka import KafkaProducer

import plugins
import logging
import settings
import utils.config
import neo4j

program = logging.getLogger(__name__)
program.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
program.addHandler(handler)

MODULE_NAME = "datadis_gather"
HDFS_PATH = f'tmp/{MODULE_NAME}'
MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/jobs/datadis:latest'
RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'


def get_all_users():
    plugins_list = plugins.get_plugins()
    users = pd.DataFrame()
    for p in plugins_list:
        program.debug(f"Getting users from source:{p}")
        tmp_df = p.get_users()
        tmp_df['source'] = p.get_source()
        users = pd.concat([users, tmp_df])
    users = pd.DataFrame(users.groupby(["username", "password", "authorized_nif"])['source'].apply(list)).reset_index()
    return users


def no_test(policy):
    archives = []
    files = []

    subprocess.call(["hdfs", "dfs", "-rm", "-r", f'{HDFS_PATH}'],
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    subprocess.call(["hdfs", "dfs", "-mkdir", "-p", f'{HDFS_PATH}'],
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    s = time.time()
    program.info("Getting users from plugins")
    users = get_all_users()
    program.info(f"Took : {time.time()-s}")

    s = time.time()
    program.info("Saving users to input file")
    users.to_csv("users.csv", sep='~', index=False, header=False)
    input_file = f'{HDFS_PATH}/users'
    partial_file = f'{HDFS_PATH}/cups'
    output_file = f"{HDFS_PATH}/output"
    subprocess.call(["hdfs", "dfs", "-copyFromLocal", 'users.csv', input_file])
    program.info(f"Took : {time.time()-s}")

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
    files.append("areducer.py#areducer.py")
    files.append("bmapper.py#bmapper.py")
    files.append("breducer.py#breducer.py")
    files.append("settings.py#settings.py")
    files.append(f"{settings.CONF_FILE}#config.json")
    with open("params.txt", "w") as param:
        param.write(f"{policy}\n")
    files.append(f"params.txt#params.txt")
    program.info(f"Took : {time.time()-s}")

    s = time.time()
    program.info("Launching part 1")
    system_call = ['mapred', 'streaming',
                   f'-Dmapreduce.map.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.reduce.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.job.name={MODULE_NAME} 1/2',
                   f'-Dmapred.reduce.tasks=10',
                   "-archives", ",".join(archives),
                   "-files", ",".join(files),
                   '-mapper', 'amapper.py',
                   '-reducer', 'areducer.py',
                   '-input', input_file,
                   '-output', partial_file]
    program.debug(" ".join(system_call))
    subprocess.call(system_call,
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    program.info(f"Took : {time.time() - s}")

    s = time.time()
    program.info("Launching part 2")
    system_call = ['mapred', 'streaming',
                   f'-Dmapreduce.map.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.reduce.env="{MOUNTS},{IMAGE},{RUNTYPE}"',
                   f'-Dmapreduce.job.name={MODULE_NAME} 2/2',
                   f'-Dmapred.reduce.tasks=10',
                   "-archives", ",".join(archives),
                   "-files", ",".join(files),
                   '-mapper', 'bmapper.py',
                   '-reducer', 'breducer.py',
                   '-input', partial_file,
                   '-output', output_file]
    subprocess.call(system_call,
                    bufsize=4096, stdout=sys.stdout, stderr=sys.stderr)
    program.info(f"Took : {time.time()-s}")

    # Whenever mapreduce ends, send all partitions a message indicating the process has ended
    config = utils.config.read_config()
    servers = [f"{config['kafka']['host']}:{config['kafka']['port']}"]
    producer = KafkaProducer(bootstrap_servers=servers,
                             value_serializer=lambda v: b64encode(pickle.dumps(v)))
    partitions = settings.TOPIC_STATIC_PARTITIONS
    kafka_message = {
        "collection_type": "FINAL_MESSAGE",
        "row_keys": "",
        "dblist": ['sime'],
        "data": {}
    }
    for partition in range(0, partitions):
        producer.send(settings.TOPIC_STATIC, value=kafka_message, partition=partition)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", "-p", choices=["last", "repair"], required=True)
    if os.getenv("PYCHARM_HOSTED") is not None:
        exit(0)
    else:
        args = ap.parse_args()
        no_test(args.policy)

