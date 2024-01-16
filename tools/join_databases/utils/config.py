import settings
import json


def read_config(conf_file=None):
    if conf_file:
        conf = json.load(open(conf_file))
    else:
        conf = json.load(open(settings.CONF_FILE))
    if 'neo4j' in conf and 'auth' in conf['neo4j']:
        conf['neo4j']['auth'] = tuple(conf['neo4j']['auth'])
    return conf
