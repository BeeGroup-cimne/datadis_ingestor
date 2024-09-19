import os
import sys
import time
import redis
import pickle
import beelib
import plugins
import logging
import argparse
import pandas as pd
from DatadisGatherer import DatadisGatherer


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
NUM_PROCESSES = 10

def get_all_users():
    plugins_list = [plugins.get_plugins()[1]]
    users = pd.DataFrame()
    for p in plugins_list:
        program.debug(f"Getting users from source:{p}")
        tmp_df = p.get_users()
        tmp_df['source'] = p.get_source()
        users = pd.concat([users, tmp_df])
    # users = users[users['authorized_nif'].isna]
    print(users)
    users['authorized_nif'] = users['authorized_nif'].apply(lambda x: x + [''] if x else x)
    users = users.explode('authorized_nif')
    users = pd.DataFrame(users.groupby(["username", "password", "authorized_nif"])['source'].apply(list)).reset_index()

    return users


def redis_barrier_sync(num_processes, red, barrier_id):
    # Marca aquest procés com a arribat
    red.incr(barrier_id)

    print('Waiting_for other processes to finish...')
    # Comprova si tots els processos han arribat
    while int(red.get(barrier_id)) < num_processes:
        print(f'Number of processes done: {int(red.get(barrier_id))}')
        time.sleep(0.2)
    print('Other processes done, execution wil continue')

    # Making sure all processes notice that they must proceed before the deletion of their key
    time.sleep(5)


def get_users(config):
    s = time.time()
    program.info("Getting users from plugins")
    users = get_all_users()
    program.info(f"Took : {time.time()-s}")
    s = time.time()
    program.info("Uploading users to Redis file")


    red = redis.Redis(**config['redis'])
    for _, row in users.iterrows():
        red.lpush('datadis.users', pickle.dumps(row.to_dict()))
    program.info(f"Took : {time.time()-s}")


def get_datadis_devices(dg, config):
    red = redis.Redis(**config['redis'])

    red.delete('datadis.barrier')
    while True:
        item = red.rpop('datadis.users')
        if not item:
            break
        item_map = pickle.loads(item)
        item_map['red'] = red
        dg.get_devices(item_map['username'], item_map['password'], item_map['authorized_nif'], item_map['source'], red)

    redis_barrier_sync(NUM_PROCESSES, red, 'datadis.barrier')


def get_datadis_data(dg, config):

    red = redis.Redis(**config['redis'])

    while True:
        item = red.rpop('datadis.devices')
        if not item:
            break
        item_map = pickle.loads(item)

        dg.get_data(item_map['user'], item_map['password'], item_map['authorized_nif'],
                    item_map['db_list'], item_map['supplies'])


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

    while time.time() < timeout:
        if redis_con.llen('datadis.users') > 0:
            break
        time.sleep(1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", "-p", choices=["last", "repair"], required=True)
    ap.add_argument("--launcher", "-l", choices=["producer", "consumer", "wait"], required=True)
    if os.getenv("PYCHARM_HOSTED") is not None:
        exit(0)
    else:
        args = ap.parse_args()
    launcher = args.launcher

    config = beelib.beeconfig.read_config('config.json')
    red = redis.Redis(**config['redis'])

    dg = DatadisGatherer()

    if launcher == 'producer':
        get_users(config)
    elif launcher == 'wait':
        wait_redis_queue(config)
    else:
        get_datadis_devices(dg, config)
        get_datadis_data(dg, config)
