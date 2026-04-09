import pymongo
from beelib import beeconfig
config = beeconfig.read_config()
mongo = pymongo.MongoClient(f"mongodb://{config['mongo']['user']}:{config['mongo']['password']}@{config['mongo']['host']}:{config['mongo']['port']}/{config['mongo']['database']}")
devices = mongo[config['mongo']['database']][config['mongo']['collection']].find({})
for device in devices:
    print(device)
    d1h = device['data_1h']
    new_d1h = {}
    for k, v in d1h.items():
        if k < "2026-02-01~2026-02-28":
            new_d1h[k] = v
    d15m = device['data_15m']
    new_d15m = {}
    for k, v in d15m.items():
        if k < "2026-02-01~2026-02-28":
            new_d15m[k] = v
    mp = device['max_power']
    new_mp = {}
    for k, v in mp.items():
        if k < "2026-02-01~2026-02-28":
            new_mp[k] = v
    device['data_1h'] = new_d1h
    device['data_15m'] = new_d15m
    device['max_power'] = new_mp
    mongo[config['mongo']['database']][config['mongo']['collection']].replace_one({"_id": device['_id']}, device, upsert=True)

