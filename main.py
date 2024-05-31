#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Jason Schultz
# Created Date: 2023-04-25
# version ='1.0'
# ---------------------------------------------------------------------------
"""Subscribes to published MQTT nodes and periodically writes them to a csv file."""
# ---------------------------------------------------------------------------
from data_extraction.client import (DataExtractionClient, DataExtractionConfig)
from mqtt_node_network.configure import MQTTBrokerConfig
import pandas as pd
import numpy as np

def main():
    # client = DataExtractionClient()
    # client.connect()
    # client.run_forever()
    # client.end_of_day(2024,5,28)

    BROKER_CONFIG = MQTTBrokerConfig(
        username ="test_user",
        password = "test_password",
        keepalive = 60,
        hostname = "test_host",
        port = 1883,
        timeout = 1,
        reconnect_attempts = 3
    )

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
        processed_output_filename = "p0_test",
        processed_output_directory = "test/test_files"
    )
    client = DataExtractionClient(
        BROKER_CONFIG, EXTRACTION_CONFIG
    )
    client.end_of_day(2024,5,29)
    


if __name__ == "__main__":
    main()
