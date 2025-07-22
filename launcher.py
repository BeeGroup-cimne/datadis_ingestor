import os
import time
import redis
import pickle
import beelib
import plugins
import logging
import argparse
import pandas as pd
from pythonjsonlogger import jsonlogger
from DatadisGatherer import get_devices_from_user_datadis, get_data, send_final_message
import dotenv

logger = logging.getLogger()
logger.setLevel("DEBUG")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter('%(asctime)s - %(levelname)s - %(name)s: %(message)s')
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


def get_all_users():
    plugins_list = plugins.get_plugins()
    users = pd.DataFrame()
    for p in plugins_list:
        logger.debug(f"Getting users from source", extra={'phase': "GATHER", 'source': p})
        tmp_df = p.get_users()
        tmp_df['source'] = p.get_source()
        tmp_df['tables'] = [p.get_tables()] * len(tmp_df)
        tmp_df['row_keys'] = [p.get_row_keys()] * len(tmp_df)

        # Covert the previous list to tuples in order to GroupBy correctly
        tmp_df["tables"] = tmp_df["tables"].apply(tuple)
        tmp_df["row_keys"] = tmp_df["row_keys"].apply(tuple)
        users = pd.concat([users, tmp_df])
    # users = users[users['authorized_nif'].isna]
    users['authorized_nif'] = users['authorized_nif'].apply(lambda x: x + [''] if x else x)
    users = users.explode('authorized_nif')
    users["authorized_nif"] = users["authorized_nif"].fillna("")
    users = pd.DataFrame(users.groupby(["username", "password", "authorized_nif", "tables", "row_keys"])['source'].apply(list)).reset_index()

    # We reverse the previous tuples transformation
    users["tables"] = users["tables"].apply(list)
    users["row_keys"] = users["row_keys"].apply(list)

    return users


def get_users(config):
    s = time.time()
    logger.info("Starting the ingestor", extra={'phase': "START"})

    logger.debug("Getting users from plugins", extra={'phase': "GATHER"})
    red = redis.Redis(**config['redis']['connection'])
    red.delete(config['redis']['users'])
    red.delete(config['redis']['devices'])
    red.delete(config['redis']['dev_tickets'])
    users = get_all_users()
    logger.debug(f"Users retrieval took: {time.time()-s} seconds", extra={'phase': "GATHER"})
    s = time.time()
    logger.debug("Uploading users to Redis", extra={'phase': "GATHER"})
    for _, row in users.iterrows():
        logger.debug("Gathered users", extra={"username": row['username'], 'phase': "GATHER"})
        red.lpush(config['redis']['users'], pickle.dumps(row.to_dict()))
    logger.debug(f"Users upload took: {time.time()-s} seconds")


def sync_processors(config, num_processes, barrier_id):
    red = redis.Redis(**config['redis']['connection'])
    red.incr(barrier_id)
    logger.debug(f"Waiting for other processes to finish", extra={'phase': "WAIT"})
    while (n := int(red.get(barrier_id))) < num_processes:
        print(f'Number of processes done: {n}')
        time.sleep(0.2)
    logger.debug(f"Other processes done, execution will continue", extra={'phase': "WAIT"})
    time.sleep(0.4)
    red.delete(barrier_id)


def get_datadis_devices(config):
    red = redis.Redis(**config['redis']['connection'])
    s = time.time()
    logger.debug("Starting devices retrieval", extra={'phase': 'GATHER'})
    while True:
        item = red.rpop(config['redis']['users'])
        if not item:
            break
        item_map = pickle.loads(item)
        supplies = get_devices_from_user_datadis(item_map['username'], item_map['password'], item_map['authorized_nif'])
        for i in range(0, len(supplies), 10):
            supply_dict = {
                'user': item_map['username'],
                'password': item_map['password'],
                'db_list': item_map['source'],
                'supplies': supplies[i:i + 10],
                'authorized_nif': item_map['authorized_nif'],
                'tables': item_map['tables'],
                'row_keys': item_map['row_keys']
            }
            logger.debug("Gathered devices", extra={"user": supply_dict['user'], 'supplies': supplies[i:i + 10],
                                                    'phase': "GATHER"})
            red.lpush(config['redis']['devices'], pickle.dumps(supply_dict))
    logger.debug(f"Devices retrieval took: {time.time()-s} seconds", extra={'phase': 'GATHER'})


def get_datadis_data(config):
    red = redis.Redis(**config['redis']['connection'])
    s = time.time()
    logger.debug("Starting data retrieval", extra={'phase': 'GATHER'})
    while True:
        item = red.rpop(config['redis']['devices'])
        if not item:
            break
        item_map = pickle.loads(item)

        get_data(item_map['user'], item_map['password'], item_map['authorized_nif'],
                    item_map['db_list'], item_map['supplies'], item_map['tables'], item_map['row_keys'], config)
    logger.debug(f"Data retrieval took: {time.time()-s} seconds", extra={'phase': 'GATHER'})



if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--launcher", "-l", choices=["producer", "consumer"], required=True)
    ap.add_argument("--num_processors", "-n", required=True)
    dotenv.load_dotenv()
    config = beelib.beeconfig.read_config()

    if os.getenv("PYCHARM_HOSTED") is not None:
        args = ap.parse_args(["-l", "producer", "-n", "1"])
    else:
        args = ap.parse_args()
        if args.launcher == 'producer':
            get_users(config)
        elif args.launcher == 'consumer':
            sync_processors(config, int(args.num_processors), config['redis']['dev_tickets'])
            get_datadis_devices(config)
            sync_processors(config, int(args.num_processors), config['redis']['data_tickets'])
            get_datadis_data(config)
            send_final_message(config)
            logger.info(f"WORKER FINISHED", extra={'phase': "END"})
