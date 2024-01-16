# Datadis description
This module will import data from Datadis and harmonize it from different sources.

The following image depicts the workflow of the script.

<img src="Datadis/module_img.svg">

It consists on a set of plugins to obtain the users and passwords, then a mapreduce is launched to gather and 
harmonize all the data.

# Mapreduce:
    STEP 1:
    MAP
    input -> user,password,source_db
    output-> id,user,password,source_db
    REDUCE
    input -> id,user,password,source_db
        - get_supplies as CUPS
    output-> id CUPS,user,password,source_db
    STEP 2:
    MAP
    input -> id CUPS,user,password,source_db
    output-> id CUPS,user,password,source_db
    REDUCE
    input -> id CUPS,user,password,source_db
        - get_contracts
        - send CUPS and CONTRACTS to kafka
        - get_consumption as CONS
        - send CONS to harmonizer
    output -> null

To run the gathering application, execute the python script with the following parameters:

```bash
python3 -m gather -so Datadis <parameters>
```

Then the following process will be running to process the data:

1- RAW DATA STORE:
   store all raw data from topics to HBASE with one FAUST APP
2- HARMONIZERS BY SOURCE
   HARMONIZE the raw data to the correct format for each source


The harmonization of the file will be done with the following mapping:

| Origin                  | Harmonization                               |
|-------------------------|---------------------------------------------|
| <field name>            | <field name> {raw field name: "Raw info"}   | 
| split(field name, sep)  | only link                                   | 
| value(<static raw>)     | <field name>                                |
| taxonomy(<field name>)  | <field name>                                |

For each import run a log document will be stored in mongo to identify the problems that may arise during the execution:

```json
{
    "user" : "the user that imported data",
    "logs": {
      "gather" : "list with the logs of the import",
      "store" : "list with the logs of the store",
      "harmonize" : "list with the logs of the harmonization"
    },
    "log_exec": "timestamp of execution"
}
```

docker buildx build --push -t docker.tech.beegroup-cimne.com/jobs/datadis . --provenance false
