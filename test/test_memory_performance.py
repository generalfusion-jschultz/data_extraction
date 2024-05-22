import pytest
from data_extraction.client import (DataExtractionClient, DataExtractionConfig)
from mqtt_node_network.client import MQTTBrokerConfig
from datetime import datetime
import time

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

@pytest.fixture
def client() -> DataExtractionClient:
    client = DataExtractionClient(
        broker_config = None,
        data_extraction_config = EXTRACTION_CONFIG
    )
    return client

def test_memory_performance(client):
    client.connect()
    i = 0

    client.run()
    while i < 100:
        time.sleep(300)
        client.start_time = datetime(2024,5,15)
        i += 1
    client.stop()


data_client = DataExtractionClient(
        broker_config = None,
        data_extraction_config = EXTRACTION_CONFIG
    )
test_memory_performance(data_client)