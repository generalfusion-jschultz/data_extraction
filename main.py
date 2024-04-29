#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Jason Schultz
# Created Date: 2023-04-25
# version ='1.0'
# ---------------------------------------------------------------------------
"""Subscribes to published MQTT nodes and periodically writes them to a csv file."""
# ---------------------------------------------------------------------------
from mqtt_node_network.initialize import initialize
from mqtt_node_network.configure import load_config
from data_extraction.client import DataExtractionClient
from mqtt_node_network.client import MQTTBrokerConfig

config, logger = initialize(
    config="./config/config.toml", secrets=".env", logger="./config/logger.yaml"
)
print("config", config, type(config))
print("logger", logger, type(logger))
# BROKER_CONFIG = config["mqtt"]["broker"]
# TOPIC_STRUCTURE = config["mqtt"]["node_network"]["topic_structure"]

# TODO: NEED TO FIGURE OUT initialize() FUNCTION TO GET PROPER config, logger VARIABLES
#       TO NOT HARDCODE THESE VALUES
BROKER_CONFIG = MQTTBrokerConfig(
    username= "gfyvrdatadash_user",
    password= "goodlinerCompressi0n!",
    keepalive= 60,
    hostname= "gfyvrdatadash",
    port = 1883,
    timeout= 5,
    reconnect_attempts= 10
)


def main():
    client = DataExtractionClient(
        BROKER_CONFIG,
        name = "data_extraction_test",
        node_id = "extraction_node_0"
    ).connect()
    client.subscribe(topic = "pzero/#")
    client.run()
    


if __name__ == "__main__":
    main()
