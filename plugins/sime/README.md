# SIME PLUGIN
This is the plugin of Datadis for SIME. 

It requires a config_file with the following fields:
```json
  {
      "neo4j": {
        "uri": "db_uri",
        "auth": ["username", "password"],
        "encrypted": true,
        "trust": "TRUST_ALL_CERTIFICATES"
      },
      "secret_password": "",
      // Needed for the harmonizer part to store the harmonized timeseries
      "hbase": {
        "connection": { 
          "host": "db",
          "port": 9090
        },
        "harmonized_data": "harmonized timeseries table",
        "row_keys": [""],
        "tables" : ""
      },
      "kafka": {
        "connection": {
          "host": "",
          "port": ""
        },
        "topic": ""
      }
  }
```

# Harmonization
Faust applications have been developed.  
That harmonizes the supply data into the corresponding Neo4j database. And stores the 
harmonized timeseries data into the corresponding database.

Execute the appropriate command based on the data source:
```bash
# ICAEN
python3 -m plugins.sime worker -l info
```

# Deployment
Add the file to the datadis-plugins-secret secret
Apply the kubernetes config in the 'kubeconfig' folder of the sime plugin
