import pytest
from data_extraction.client import (DataExtractionClient, DataExtractionConfig)
from mqtt_node_network.client import MQTTBrokerConfig
from datetime import datetime
import time
from prometheus_client import REGISTRY

#---------------Client initialization------------------------------------------
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


#---------------------------Test functions-----------------------------------------
def test_small_file(client):
    client.connect()
    i = 0
    before = after = None

    client.run()
    while i < 30:
        time.sleep(10)
        client.start_time = datetime(2024,5,15)
        before = after
        after = REGISTRY.get_sample_value("client_memory_usage")
        if (before is not None) and (after is not None):
            assert (before < 300) or (after < 300)
        i += 1
    client.stop()


# Assertion runs into issues if time.sleep is too low as memory can increase for more than
# two Gauge reads while reading and processing the csv file
def test_large_file(client):
    client.connect()
    i = 0
    before = after = None

    client.run()
    while i < 30:
        time.sleep(40)
        client.start_time = datetime(2024,5,22)
        before = after
        after = REGISTRY.get_sample_value("client_memory_usage")
        if (before is not None) and (after is not None):
            assert (before < 300) or (after < 300)
        i += 1
    client.stop()


#-----------------------------Functions to generate a topics csv file-------------------------------------------- 
def generate_topic_and_id():
    topics_list = [
        "prototype-zero/normal/sensor/pressure_lower_enclosure/pumpcart_pressure/vacuum_gauge_hornet-voltage",
        "prototype-zero/normal/sensor/pressure_lower_enclosure/pumpcart_pressure/vacuum_gauge_hornet-torr",
        "prototype-zero/normal/sensor/pressure_lower_enclosure/pumpcart_pressure/vacuum_gauge_stinger-voltage",
        "prototype-zero/normal/sensor/pressure_lower_enclosure/pumpcart_pressure/vacuum_gauge_stinger-torr"
    ]
    ids_list = [
        "sensor/pumpcart_pressure/vacuum_gauge_hornet-voltage",
        "sensor/pumpcart_pressure/vacuum_gauge_hornet-torr",
        "sensor/pumpcart_pressure/vacuum_gauge_stinger-voltage",
        "sensor/pumpcart_pressure/vacuum_gauge_stinger-torr"
    ]
    assert len(topics_list) == len(ids_list)
    topics = topics_list.copy()
    ids = ids_list.copy()
    while True:
        if len(topics) == 0:
            topics = topics_list.copy()
            ids = ids_list.copy()
        yield (topics.pop(), ids.pop())


def create_csv(client: DataExtractionClient):
    from buffered.buffer import Buffer
    import random

    generator = generate_topic_and_id()
    buffer = Buffer()
    for _ in range(2_000_001):
        (topic, id_str) = next(generator)
        row = {
            "time": datetime.now(),
            "topic": topic,
            "id": id_str,
            "value": random.random()
        }
        buffer.append(row)

        if len(buffer) > 4_000:
            client.update_csv(buffer.dump(), "./test/test_files/20240522-test.csv")
    client.update_csv(buffer.dump(), "./test/test_files/20240522-test.csv")


def test_generator(client: DataExtractionClient):
    test_list = client.get_unique_ids("./test/test_files/20240522-test.csv")
    print(test_list)
    assert 1 == 2