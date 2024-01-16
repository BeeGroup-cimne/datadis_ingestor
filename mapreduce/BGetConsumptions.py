#!/usr/bin/env python3
import pickle
import sys
from base64 import b64decode, b64encode
from datetime import datetime
from functools import partial
import pandas as pd
import pytz
from kafka import KafkaProducer
import settings
from mapreduce import Map, Reduce
from dateutil.relativedelta import relativedelta
from beedis import ENDPOINTS, datadis
import pymongo

from utils import utils


class ReadSupplies(Map):
    def __init__(self):
        self.records = 0

    def map(self, line):
        print(line)


class GetConsumption(Reduce):
    TZ = pytz.timezone("Europe/Madrid")

    def __init__(self):
        self.records = 0
        self.config = utils.config.read_config("config.json")
        with open("params.txt") as param:
            self.config['policy'] = param.readline().strip()

        self.source = "datadis"

    @staticmethod
    def get_values_period(init_time, end_time, freq):
        init_time = GetConsumption.TZ.localize(init_time)
        end_time = GetConsumption.TZ.localize(end_time)
        return len(pd.date_range(init_time, end_time, freq=freq))

    @staticmethod
    def parse_max_power_chunk(max_power):
        if len(max_power) <= 0:
            return list()
        try:
            df_consumption = pd.DataFrame(max_power)
            # Cast datetime64[ns] to timestamp (int64)
            df_consumption.set_index('datetime', inplace=True)
            df_consumption.sort_index(inplace=True)
            for c in [x for x in df_consumption.columns if x.startswith("datetime_")]:
                df_consumption[c] = df_consumption[c].astype('int64') // 10 ** 9

            df_consumption['timestamp'] = df_consumption.index.astype('int64') // 10 ** 9
            m_power = df_consumption.to_dict(orient='records')
            for x in m_power:
                dts = [dt for dt in x.keys() if dt.startswith("datetime_")]
                x['cups'] = x.pop('cups_period_')
                for c in dts:
                    if x[c] == -9223372037:  # NaT representation in timestamp
                        period = c.split("_")[2]
                        x.pop(f"datetime_period_{period}")
                        x.pop(f"maxPower_period_{period}")

            return m_power
        except Exception as e:
            return list()

    @staticmethod
    def parse_consumption_chunk(consumption):
        if len(consumption) <= 0:
            return list()
        try:
            df_consumption = pd.DataFrame(consumption)
            df_consumption.index = df_consumption['datetime']
            df_consumption.sort_index(inplace=True)
            # Cast datetime64[ns] to timestamp (int64)
            df_consumption['timestamp'] = df_consumption['datetime'].astype('int64') // 10 ** 9
            return df_consumption.to_dict(orient='records')
        except Exception as e:
            return list()

    data_types_dict = {
        "EnergyConsumptionGridElectricity_PT1H": {
            "mongo_collection": "data_1h",
            "type_data": "timeseries",
            "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
            "measurement_type": "0",
            "endpoint": ENDPOINTS.GET_CONSUMPTION,
            "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
            "elements_in_period": partial(get_values_period, freq="h"),
            "parser": parse_consumption_chunk,
        },
        "EnergyConsumptionGridElectricity_PT15M": {
            "mongo_collection": "data_15m",
            "type_data": "timeseries",
            "freq_rec": relativedelta(day=31, hour=23, minute=59, second=59),
            "measurement_type": "1",
            "endpoint": ENDPOINTS.GET_CONSUMPTION,
            "params": ["cups", "distributor_code", "start_date", "end_date", "measurement_type", "point_type"],
            "elements_in_period": partial(get_values_period, freq="15t"),
            "parser": parse_consumption_chunk,
        },
        "Power_P1M": {
            "mongo_collection": "max_power",
            "type_data": "timeseries",
            "freq_rec": relativedelta(months=5) + relativedelta(day=31, hour=23, minute=59, second=59),
            "endpoint": ENDPOINTS.GET_MAX_POWER,
            "params": ["cups", "distributor_code", "start_date", "end_date"],
            "elements_in_period": partial(get_values_period, freq="1M"),
            "parser": parse_max_power_chunk,
        }
    }

    def save_datadis_data(self, topic, collection_type, key, data, row_keys, dblist, **kwargs):
        servers = [f"{self.config['kafka']['host']}:{self.config['kafka']['port']}"]
        producer = KafkaProducer(bootstrap_servers=servers,
                                 value_serializer=lambda v: b64encode(pickle.dumps(v)))
        try:
            kafka_message = {
                "collection_type": collection_type,
                "row_keys": row_keys,
                "dblist": dblist,
                "data": data
            }
            kafka_message.update(kwargs)
            producer.send(topic, key=key.encode('utf-8'), value=kafka_message)
        except Exception as e:
            print(e, file=sys.stderr)

    def parse_arguments(self, row, type_params, date_ini, date_end):
        arguments = {}
        for a in type_params['params']:
            if a == "cups":
                arguments["cups"] = row["cups"]
            elif a == "distributor_code":
                arguments["distributor_code"] = row['distributorCode']
            elif a == "start_date":
                arguments["start_date"] = date_ini
            elif a == "end_date":
                arguments["end_date"] = date_end
            elif a == "measurement_type":
                arguments["measurement_type"] = type_params['measurement_type']
            elif a == "point_type":
                arguments["point_type"] = str(row["pointType"])
        return arguments

    def download_chunk(self, supply, type_params, status):
        try:
            date_ini_req = status['date_ini_block'].date()
            date_end_req = status['date_end_block'].date()
            print(f"downloading {supply['cups']} from {date_ini_req} to {date_end_req}", file=sys.stderr)
            kwargs = self.parse_arguments(supply, type_params, date_ini_req, date_end_req)
            consumption = datadis.datadis_query(type_params['endpoint'], **kwargs)
            if not consumption:
                raise Exception(f"No data could be found")
            return consumption
        except Exception as e:
            print(e, file=sys.stderr)
            return list()

    def download_device(self, supply, device, datadis_devices, dblist):
        for data_type, type_params in self.data_types_dict.items():
            m_property, freq = data_type.split("_")
            if type_params['type_data'] == "timeseries":
                if self.config['policy'] == "last":
                    try:
                        # get last chunk
                        status = list(device[type_params["mongo_collection"]].values())[-1]
                    except IndexError as e:
                        continue
                    # check if chunk is in current time
                    if not (status['date_ini_block'] <= datetime.today()
                            <= status['date_end_block']):
                        continue
                    data = self.download_chunk(supply, type_params, status)
                    print("reporter:counter: CUSTOM, gather,1", file=sys.stderr)
                    data_df = type_params['parser'](data)
                    if len(data_df) > 0:
                        self.save_datadis_data(settings.TOPIC_TS, "timeseries", supply['cups'],
                                               data_df, ["cups", "timestamp"], dblist, property=m_property, freq=freq)
                        status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(pytz.UTC)
                        status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                            tz_localize(pytz.UTC)
                    # store status info
                    status['values'] = len(data_df)
                    print("reporter:counter: CUSTOM, end,1", file=sys.stderr)
                if self.config['policy'] == "repair":
                    # get all incomplete chunks
                    status_list = [x for x in device[type_params["mongo_collection"]].values()
                                   if x['values'] < x['total'] and x['retries'] > 0 and
                                   x['date_ini_block'] > datetime.today() - relativedelta(years=1, months=11)]
                    for status in status_list:
                        data = self.download_chunk(supply, type_params, status)
                        print("reporter:counter: CUSTOM, gather,1", file=sys.stderr)
                        data_df = type_params['parser'](data)
                        if len(data_df) > 0:
                            self.save_datadis_data(settings.TOPIC_TS, "timeseries", supply['cups'],
                                                   data_df, ["cups", "timestamp"], dblist, property=m_property, freq=freq)
                            status['date_min'] = pd.to_datetime(data_df[0]['timestamp'], unit="s").tz_localize(
                                pytz.UTC)
                            status['date_max'] = pd.to_datetime(data_df[-1]['timestamp'], unit="s"). \
                                tz_localize(pytz.UTC)
                        else:
                            status['retries'] -= 1
                        status['values'] = len(data_df)
        # store status info
        datadis_devices.replace_one({"_id": device['_id']}, device, upsert=True)
        print("reporter:counter: CUSTOM, end,1", file=sys.stderr)

    def get_device(self, supply, datadis_devices):
        """
        get or create the data chuncks from mongo to update the data
        :param supply: the supply to get chunks
        :param datadis_devices: the mongo database where logs are stored
        :return: the device chunks created
        """
        try:
            device = datadis_devices.find_one({"_id": supply['cups']})
        except IndexError:
            device = None
        if not device:
            # if there is no log document create a new one
            device = {
                "_id": supply['cups']
            }
        has_init_date = True
        try:
            date_ini = datetime.strptime(supply['validDateFrom'], '%Y/%m/%d')
        except ValueError:
            has_init_date = False
            date_ini = datetime(2018, 1, 1)
        now = datetime.today().date() + relativedelta(day=31, hour=23, minute=59, second=59)
        try:
            date_end = datetime.strptime(supply['validDateTo'][:-2], '%Y/%m') + \
                       relativedelta(day=31, hour=23, minute=59, second=59)
            if date_end <= now:
                date_end = date_end
            else:
                raise Exception()
        except Exception as e:
            date_end = now
        for t, type_params in [(x, y) for x, y in self.data_types_dict.items() if y["type_data"] == "timeseries"]:
            if type_params["mongo_collection"] not in device:
                device[type_params["mongo_collection"]] = {}
            loop_date_ini = date_ini
            while loop_date_ini < date_end:
                current_ini = loop_date_ini
                current_end = current_ini + type_params['freq_rec']
                k = "~".join([current_ini.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")])
                date_ini_block = current_ini
                date_end_block = current_end
                if k not in device[type_params["mongo_collection"]]:
                    device[type_params["mongo_collection"]].update({
                        k: {
                            "has_ini_date": has_init_date,
                            "date_ini_block": date_ini_block,
                            "date_end_block": date_end_block,
                            "values": 0,
                            "total": type_params['elements_in_period'](date_ini_block, date_end_block +
                                                                       relativedelta(seconds=1)),
                            "retries": 6,
                        }
                    })
                loop_date_ini = current_end + relativedelta(seconds=1)
        return device

    def reduce(self, k, v):
        _, user, password, nif, dblist = k.strip().split('~')
        dblist = eval(dblist)
        try:
            datadis.connection(username=user, password=password, timeout=1000)
            for pickled_supply in v:
                supply = pickle.loads(b64decode(eval(pickled_supply.strip())))
                contracts_dd = datadis.datadis_query(ENDPOINTS.GET_CONTRACT, cups=supply['cups'],
                                                     distributor_code=supply['distributorCode'], authorized_nif=nif)
                contract = pd.DataFrame.from_records(contracts_dd).set_index('startDate')\
                    .reset_index().iloc[-1].to_dict()
                supply['nif'] = nif if nif else user
                mongo = pymongo.MongoClient(
                    f"mongodb://{self.config['mongo']['user']}:{self.config['mongo']['password']}@"
                    f"{self.config['mongo']['host']}:{self.config['mongo']['port']}/{self.config['mongo']['table']}")
                datadis_devices = mongo[self.config['mongo']['table']][self.config['mongo']['collection']]
                device = self.get_device(supply, datadis_devices)
                self.download_device(supply, device, datadis_devices, dblist)
                self.save_datadis_data(settings.TOPIC_STATIC, "supplies", supply['cups'], supply, ["cups"], dblist)
                self.save_datadis_data(settings.TOPIC_STATIC, "contracts", contract['cups'], contract, ["cups"], dblist)
        except Exception as e:
            print(f"{user}: {e}", file=sys.stderr)
