# data_exctraction
### Backs up MQTT subscription data on csv files

This module subscribes to a given MQTT topics list and periodically backs data up to a csv file. At the end of the day it processes the csv file and outputs a formatted csv file of all the data.


```
# If running with a config/config.toml file to load parameters
# for both Config classes
from data_extraction.client import DataExtractionClient

client = DataExtractionClient()
client.connect()
client.run_forever()

# Manual setup
from data_extraction.client import (DataExtractionClient, DataExtractionConfig)
from mqtt_node_network.client import MQTTBrokerConfig
from mqtt_node_network.initialize import initialize

config = initialize(
    config="./config/config.toml", secrets=".env", logger="./config/logger.yaml"
)
BROKER_CONFIG = config["mqtt"]["broker"]

EXTRACTION_CONFIG = DataExtractionConfig(
    name = "test_name",
    node_id = "test_node_id",
    max_buffer_length = 1000,
    max_buffer_time = 10,
    subscriptions = "machine/#",
    topic_structure = "machine/permission/category/module/measurement/field*",
    id_structure = "category/measurement/field*",
    resample_time = 1,
    nan_limit = 4,
    output_filename = "test",
    output_directory = "test/test_files",
    processed_output_filename = "processed_test",
    processed_output_directory = "test/test_files"
)
client = DataExtractionClient(
    broker_config = BROKER_CONFIG,
    data_extraction_config = EXTRACTION_CONFIG
)
```
