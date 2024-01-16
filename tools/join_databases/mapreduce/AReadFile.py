#!/usr/bin/env python3
import pickle
import sys
from base64 import b64encode

import utils.hbase
from mapreduce import Map, Reduce
from beedis import ENDPOINTS, datadis


class ReadFile(Map):
    def __init__(self):
        self.config = utils.config.read_config("config.json")
        with open("params.txt") as param:
            self.h_table_name = param.readline().strip()
        self.batch = []

    def finish(self):
        utils.hbase.save_to_hbase(self.batch, self.h_table_name, self.config['hbase']['connection'],
                                  [("info", 'all')], ["hash", "start"])

    def map(self, line):
        hash, start, consumptionKWh, datetime, obtainMethod = line.replace("\x5CN", "").split('\t')
        print(f"{hash}, {start}", file=sys.stderr)
        self.batch.append({'hash': hash, 'start': start, 'consumptionKWh': consumptionKWh, 'datetime': datetime,
                           'obtainMethod': obtainMethod})
        if len(self.batch) >= 100000:
            utils.hbase.save_to_hbase(self.batch, self.h_table_name, self.config['hbase']['connection'],
                                      [("info", 'all')], ["hash", "start"])
            self.batch = []
