import os
import time
import redis
import pickle
import beelib
import plugins
import logging
import argparse
import pandas as pd
from neo4j import GraphDatabase
from pythonjsonlogger import jsonlogger
from DatadisGatherer import DatadisGatherer


logger = logging.getLogger()
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

MODULE_NAME = "datadis_gather"
HDFS_PATH = f'tmp/{MODULE_NAME}'
MOUNTS = 'YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/hadoop_stack:/hadoop_stack:ro'
IMAGE = 'YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=docker.tech.beegroup-cimne.com/jobs/datadis:latest'
RUNTYPE = 'YARN_CONTAINER_RUNTIME_TYPE=docker'
NUM_PROCESSES = 10


def get_all_users():
    plugins_list = plugins.get_plugins()
    sime = next((cls for cls in plugins_list if cls.__name__ == "SIMEImport"), None)
    plugins_list = [sime]
    users = pd.DataFrame()
    for p in plugins_list:
        logger.debug(f"Getting users from source", extra={'phase': "GATHER", 'source': p})
        tmp_df = p.get_users()
        tmp_df['source'] = p.get_source()
        users = pd.concat([users, tmp_df])
    # users = users[users['authorized_nif'].isna]
    users['authorized_nif'] = users['authorized_nif'].apply(lambda x: x + [''] if x else x)
    users = users.explode('authorized_nif')
    users = pd.DataFrame(users.groupby(["username", "password", "authorized_nif"])['source'].apply(list)).reset_index()

    return users


def redis_barrier_sync(num_processes, red, barrier_id):
    # Marca aquest procés com a arribat
    red.incr(barrier_id)

    logger.debug(f"Waiting for other processes to finish", extra={'phase': "WAIT"})
    # Comprova si tots els processos han arribat
    while int(red.get(barrier_id)) < num_processes:
        print(f'Number of processes done: {int(red.get(barrier_id))}')
        time.sleep(0.2)
    logger.debug(f"Other processes done, execution wil continue", extra={'phase': "WAIT"})

    # Making sure all processes notice that they must proceed before the deletion of their key
    time.sleep(5)


def get_users(config):
    s = time.time()
    logger.info("Starting the ingestor", extra={'phase': "START"})

    logger.debug("Getting users from plugins", extra={'phase': "GATHER"})
    users = get_all_users()
    logger.debug(f"Users retrieval took: {time.time()-s} seconds", extra={'phase': "GATHER"})
    s = time.time()
    logger.debug("Uploading users to Redis file", extra={'phase': "GATHER"})

    red = redis.Redis(**config['redis'])
    for _, row in users.iterrows():
        logger.debug("Gathered users", extra={"username": row['username'], 'phase': "GATHER"})
        red.lpush('datadis.users', pickle.dumps(row.to_dict()))
    logger.debug(f"Users upload took: {time.time()-s} seconds")


def get_datadis_devices(dg, config):
    red = redis.Redis(**config['redis'])
    s = time.time()
    logger.debug("Starting devices retrieval", extra={'phase': 'GATHER'})
    red.delete('datadis.barrier')
    while True:
        item = red.rpop('datadis.users')
        if not item:
            break
        item_map = pickle.loads(item)
        item_map['red'] = red
        dg.get_devices(item_map['username'], item_map['password'], item_map['authorized_nif'], item_map['source'], red)
    logger.debug(f"Devices retrieval took: {time.time()-s} seconds", extra={'phase': 'GATHER'})
    redis_barrier_sync(NUM_PROCESSES, red, 'datadis.barrier')


def get_datadis_data(dg, config):
    red = redis.Redis(**config['redis'])
    s = time.time()
    logger.debug("Starting data retrieval", extra={'phase': 'GATHER'})
    while True:
        item = red.rpop('datadis.devices')
        if not item:
            break
        item_map = pickle.loads(item)

        dg.get_data(item_map['user'], item_map['password'], item_map['authorized_nif'],
                    item_map['db_list'], item_map['supplies'])
    logger.debug(f"Data retrieval took: {time.time()-s} seconds", extra={'phase': 'GATHER'})
    redis_barrier_sync(NUM_PROCESSES, red, 'datadis.barrier')


def nifs_multisource(config):
    redis_client = redis.StrictRedis(**config['redis'])
    lock_key = "my_lock"

    # Try acquiring the lock
    if redis_client.set(lock_key, "lock", nx=True, ex=180):
        logger.debug(f"Pod solving multi source issue", extra={'phase': 'GATHER'})
        # Since all the data is being sent to Kafka, there's a chance that while the multi sources solver is being
        # executed, data is still being harmonized, because of that, we have to put this process to sleep for a while
        time.sleep(120)
        plugins_list = plugins.get_plugins()
        sime = next((cls for cls in plugins_list if cls.__name__ == "SIMEImport"), None)
        plugins_list = [sime]
        for p in plugins_list:
            p.solve_multisources()
        redis_client.delete(lock_key)  # Release the lock
        logger.debug(f"Pod finished solving multi source issue", extra={'phase': 'GATHER'})

    else:
        logger.debug(f"Another pod already solving multi source issue", extra={'phase': 'GATHER'})


def empty_users(red):
    item = red.rpop('datadis.users')
    while item:
        item = red.rpop('datadis.users')


def empty_devices(red):
    item = red.rpop('datadis.devices')
    while item:
        item = red.rpop('datadis.devices')


def wait_redis_queue(config):
    redis_con = redis.Redis(**config['redis'])
    timeout = time.time() + 60 * 10  # timeout 10 minutes
    logger.debug(f"Waiting for starter to store data in queue", extra={'phase': "WAIT"})

    while time.time() < timeout:
        if redis_con.llen('datadis.users') > 0:
            break
        time.sleep(1)
    logger.debug(f"Data already in queue, new process will start", extra={'phase': "WAIT"})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", "-p", choices=["last", "repair"], required=True)
    ap.add_argument("--launcher", "-l", choices=["producer", "consumer", "wait"], required=True)
    if os.getenv("PYCHARM_HOSTED") is not None:
        exit(0)
    else:
        args = ap.parse_args()
    launcher = args.launcher
    policy = args.policy

    config = beelib.beeconfig.read_config('config.json')
    red = redis.Redis(**config['redis'])

    if launcher == 'producer':
        get_users(config)
    elif launcher == 'wait':
        wait_redis_queue(config)
    else:
        dg = DatadisGatherer(policy)

        get_datadis_devices(dg, config)
        get_datadis_data(dg, config)
        nifs_multisource(config)
        logger.info(f"WORKER FINISHED", extra={'phase': "END"})
