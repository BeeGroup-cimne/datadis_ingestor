#!/usr/bin/env python3
import pickle
import sys
from base64 import b64encode

from mapreduce import Map, Reduce
from beedis import ENDPOINTS, datadis


class ReadUsers(Map):
    def __init__(self):
        self.records = 0

    def map(self, line):
        self.records += 1
        print(f"{self.records}{Map.SEP}{line}")


class GetUsers(Reduce):
    def __init__(self):
        self.records = 0

    def reduce(self, k, v):
        for u in v:
            user, password, authorized_nif, db_list = u.strip().split("~")
            try:
                print(f"{user} start", file=sys.stderr)
                datadis.connection(username=user, password=password, timeout=1000)
                print("reporter:counter: CUSTOM, connection,1", file=sys.stderr)
                supplies = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES)
                print("reporter:counter: CUSTOM, get_supplies,1", file=sys.stderr)
                if not supplies:
                    supplies = []
                authorized_supplies = {}
                if authorized_nif:
                    for nif in authorized_nif.split(","):
                        authorized_supplies[nif] = datadis.datadis_query(ENDPOINTS.GET_SUPPLIES, authorized_nif=nif)
                        # print(f"SUMMARY|{user}\t{nif}\t{len(authorized_supplies[nif])}\tno")
                        print("reporter:counter: CUSTOM, get_supplies_auth,1", file=sys.stderr)
            except Exception as e:
                # print(f"SUMMARY|{user}\t\t0\t{e}")
                continue
            # print(f"SUMMARY|{user}\t\t{len(supplies)}\tno")
            print(f"{user} end", file=sys.stderr)
            for supply in supplies:
                if self.records % 10:
                    self.records += 1
                key = f"{self.records}~{user}~{password}~~{db_list}"
                value = pickle.dumps(supply)
                value = b64encode(value)
                print(f"{key}{Reduce.SEP}{value}")
                # print(f"CUPS|{supply['cups']}\t{supply['address']}\t{supply['municipality']}\t{user}\t")
                print("reporter:counter: CUSTOM, supplies,1", file=sys.stderr)
            for nif in authorized_supplies:
                for supply in authorized_supplies[nif]:
                    if self.records % 10:
                        self.records += 1
                    key = f"{self.records}~{user}~{password}~{nif}~{db_list}"
                    value = pickle.dumps(supply)
                    value = b64encode(value)
                    print(f"{key}{Reduce.SEP}{value}")
                    # print(f"CUPS|{supply['cups']}\t{supply['address']}\t{supply['municipality']}\t{user}\t{nif}")
                    # print("reporter:counter: CUSTOM, supplies_auth,1", file=sys.stderr)

