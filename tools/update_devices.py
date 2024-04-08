import pymongo
from utils import utils
config = utils.config.read_config()
mongo = pymongo.MongoClient(f"mongodb://{config['mongo']['user']}:{config['mongo']['password']}@{config['mongo']['host']}:{config['mongo']['port']}/{config['mongo']['table']}")
devices = mongo[config['mongo']['table']][config['mongo']['collection']].find({})
for device in devices:
    d1h = device['data_1h']
    new_d1h = {}
    for k, v in d1h.items():
        if k < "2022-02-01~2022-02-28":
            new_d1h[k] = v
    d15m = device['data_15m']
    new_d15m = {}
    for k, v in d15m.items():
        if k < "2022-02-01~2022-02-28":
            new_d15m[k] = v
    mp = device['max_power']
    new_mp = {}
    for k, v in mp.items():
        if k < "2022-07-01~2022-12-31":
            new_mp[k] = v
    device['data_1h'] = new_d1h
    device['data_15m'] = new_d15m
    device['max_power'] = new_mp
    mongo[config['mongo']['table']][config['mongo']['collection']].replace_one({"_id": device['_id']}, device, upsert=True)

