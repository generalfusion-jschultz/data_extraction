import pytest
from data_extraction.client import (DataExtractionClient, DataExtractionConfig)
from mqtt_node_network.client import MQTTBrokerConfig
from datetime import datetime
import numpy as np
import pandas as pd
from paho.mqtt.client import MQTTMessage
import time


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

@pytest.fixture
def client() -> DataExtractionClient:
    client = DataExtractionClient(
        broker_config = BROKER_CONFIG,
        data_extraction_config = EXTRACTION_CONFIG
    )
    return client


def test_client_initialization(client):  
    assert len(client.buffer) == 0
    assert client.buffer.empty() is True
    assert client.name == EXTRACTION_CONFIG.name
    assert client.node_id == EXTRACTION_CONFIG.node_id
    assert client.max_buffer == EXTRACTION_CONFIG.max_buffer_length
    assert client._username == BROKER_CONFIG.username
    assert client._password == BROKER_CONFIG.password
    assert client.port == BROKER_CONFIG.port
    assert client.buffer_time_interval == EXTRACTION_CONFIG.max_buffer_time
    assert client.resample_time_seconds == EXTRACTION_CONFIG.resample_time
    assert client.output_filename == EXTRACTION_CONFIG.output_filename
    assert client.processed_filename == EXTRACTION_CONFIG.processed_output_filename
    assert client.output_directory == EXTRACTION_CONFIG.output_directory
    assert client.processed_directory == EXTRACTION_CONFIG.processed_output_directory
    assert client.nan_limit == EXTRACTION_CONFIG.nan_limit


def test_data_config_fails():
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = None,
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = "4",
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000.5,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = ['machine/#', 5],
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = 1,
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = 1,
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = 1,
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = 1,
            processed_output_filename = "p0_test",
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = 1,
            processed_output_directory = "test/test_files"
        )
    with pytest.raises(TypeError):
        data_config = DataExtractionConfig(
            name = "test_name",
            node_id = "test_node_id",
            max_buffer_length = 1000,
            max_buffer_time = 10,
            subscriptions = 'machine/#',
            topic_structure = "machine/permission/category/module/measurement/field*",
            id_structure = "category/measurement/field*",
            resample_time = 1,
            nan_limit = 4,
            output_filename = "test",
            output_directory = "test/test_files",
            processed_output_filename = "p0_test",
            processed_output_directory = 1
        )


def test_on_message(mocker, client):
    class MockMessage:
        def __init__(self):
            self.time = datetime.now()
            self.topic = "p0/normal/sensor/enclosure/pyrometer/temperature"
            self.id = "sensor/pyrometer/temperature"
            self.value = 5
    mocker.patch("mqtt_node_network.node.MQTTNode.on_message", return_value = None)
    
    mock_message = MockMessage()
    mocker.patch("data_extraction.client.DataExtractionClient.check_message_value", return_value = mock_message.value)
    client.on_message(None, None, mock_message)

    assert len(client.buffer) == 1
    assert (client.buffer[0]["time"] - mock_message.time).total_seconds() == pytest.approx(0, abs = 1)
    assert client.buffer[0]["topic"] == mock_message.topic
    assert client.buffer[0]["id"] == mock_message.id
    assert client.buffer[0]["value"] == mock_message.value


#----------Testing reading in files and processing the data------------
def test_single_line_topics_file(client):
    expected_results = {
        "pyrometer/ir_01": {
            datetime(2024,5,1,15,0,0,0): 20.0
        }
    }
    
    df = client.obtain_df(2024, 5, 1)
    df = client.process_data_pandas(df)
    assert df.to_dict() == expected_results


def assert_dicts(actual: dict[str, dict], expected: dict[str, dict], debug: bool = True) -> None:
    for (column_id, column_values) in actual.items():
        for (timestamp, value) in column_values.items():
            if debug:
                print(value, expected[column_id][timestamp])
                print(type(value))
            # try:
            #     value = float(value)
            # except ValueError:
            #     pass
            if isinstance(value, float):
                if np.isnan(value):
                    assert np.isnan(expected[column_id][timestamp])
                else:
                    assert value == pytest.approx(expected[column_id][timestamp], abs = 0.001)
            elif isinstance(value, str):
                assert value == expected[column_id][timestamp]
            else:
                assert False


def test_nonexistent_topics_file(client):
    df = client.end_of_day(2024, 4, 12)
    assert df is None


def test_standard_topics_file(client):
    expected_interp = {
        "pyrometer/ir_01": {
            datetime(2024,5,2,12,0,0,000000): 20.00,
            datetime(2024,5,2,12,0,0,250000): 20.25,
            datetime(2024,5,2,12,0,0,500000): 20.50,
            datetime(2024,5,2,12,0,0,750000): 20.75,
            datetime(2024,5,2,12,0,1,000000): 21.00,
            datetime(2024,5,2,12,0,1,250000): 21.25,
            datetime(2024,5,2,12,0,1,500000): 21.50,
            datetime(2024,5,2,12,0,1,750000): 21.75,
            datetime(2024,5,2,12,0,2,000000): 22.00,
            datetime(2024,5,2,12,0,2,250000): 22.25,
            datetime(2024,5,2,12,0,2,500000): 22.50,
            datetime(2024,5,2,12,0,2,750000): 22.75,
            datetime(2024,5,2,12,0,3,000000): 23.00,
            datetime(2024,5,2,12,0,3,250000): 23.00,
            datetime(2024,5,2,12,0,3,500000): 23.00,
            datetime(2024,5,2,12,0,3,750000): 23.00,
        },
        "pyrometer/ir_02": {
            datetime(2024,5,2,12,0,0,000000): np.nan,
            datetime(2024,5,2,12,0,0,250000): 30.00,
            datetime(2024,5,2,12,0,0,500000): 30.25,
            datetime(2024,5,2,12,0,0,750000): 30.50,
            datetime(2024,5,2,12,0,1,000000): 30.75,
            datetime(2024,5,2,12,0,1,250000): 31.00,
            datetime(2024,5,2,12,0,1,500000): 31.25,
            datetime(2024,5,2,12,0,1,750000): 31.50,
            datetime(2024,5,2,12,0,2,000000): 31.75,
            datetime(2024,5,2,12,0,2,250000): 32.00,
            datetime(2024,5,2,12,0,2,500000): 32.25,
            datetime(2024,5,2,12,0,2,750000): 32.50,
            datetime(2024,5,2,12,0,3,000000): 32.75,
            datetime(2024,5,2,12,0,3,250000): 33.00,
            datetime(2024,5,2,12,0,3,500000): 33.00,
            datetime(2024,5,2,12,0,3,750000): 33.00,
        },
        "pressure/stinger": {
            datetime(2024,5,2,12,0,0,000000): np.nan,
            datetime(2024,5,2,12,0,0,250000): np.nan,
            datetime(2024,5,2,12,0,0,500000): 2.0,
            datetime(2024,5,2,12,0,0,750000): 2.025,
            datetime(2024,5,2,12,0,1,000000): 2.050,
            datetime(2024,5,2,12,0,1,250000): 2.075,
            datetime(2024,5,2,12,0,1,500000): 2.1,
            datetime(2024,5,2,12,0,1,750000): 2.125,
            datetime(2024,5,2,12,0,2,000000): 2.150,
            datetime(2024,5,2,12,0,2,250000): 2.175,
            datetime(2024,5,2,12,0,2,500000): 2.2,
            datetime(2024,5,2,12,0,2,750000): 2.225,
            datetime(2024,5,2,12,0,3,000000): 2.250,
            datetime(2024,5,2,12,0,3,250000): 2.275,
            datetime(2024,5,2,12,0,3,500000): 2.3,
            datetime(2024,5,2,12,0,3,750000): 2.3,
            },
        "pressure/hornet": {
            datetime(2024,5,2,12,0,0,000000): np.nan,
            datetime(2024,5,2,12,0,0,250000): np.nan,
            datetime(2024,5,2,12,0,0,500000): np.nan,
            datetime(2024,5,2,12,0,0,750000): 1.0,
            datetime(2024,5,2,12,0,1,000000): 1.025,
            datetime(2024,5,2,12,0,1,250000): 1.050,
            datetime(2024,5,2,12,0,1,500000): 1.075,
            datetime(2024,5,2,12,0,1,750000): 1.1,
            datetime(2024,5,2,12,0,2,000000): 1.125,
            datetime(2024,5,2,12,0,2,250000): 1.150,
            datetime(2024,5,2,12,0,2,500000): 1.175,
            datetime(2024,5,2,12,0,2,750000): 1.2,
            datetime(2024,5,2,12,0,3,000000): 1.225,
            datetime(2024,5,2,12,0,3,250000): 1.250,
            datetime(2024,5,2,12,0,3,500000): 1.275,
            datetime(2024,5,2,12,0,3,750000): 1.3,
        },
    }
    expected_resample = {
        "pyrometer/ir_01": {
            datetime(2024,5,2,12,0,0): 20.375,
            datetime(2024,5,2,12,0,1): 21.375,
            datetime(2024,5,2,12,0,2): 22.375,
            datetime(2024,5,2,12,0,3): 23.0,
        },
        "pyrometer/ir_02": {
            datetime(2024,5,2,12,0,0,): 30.25,
            datetime(2024,5,2,12,0,1,): 31.125,
            datetime(2024,5,2,12,0,2,): 32.125,
            datetime(2024,5,2,12,0,3,): 32.9375,
        },
        "pressure/stinger": {
            datetime(2024,5,2,12,0,0): 2.0125,
            datetime(2024,5,2,12,0,1): 2.0875,
            datetime(2024,5,2,12,0,2): 2.1875,
            datetime(2024,5,2,12,0,3): 2.28125,
            },
        "pressure/hornet": {
            datetime(2024,5,2,12,0,0): 1.0,
            datetime(2024,5,2,12,0,1): 1.0625,
            datetime(2024,5,2,12,0,2): 1.1625,
            datetime(2024,5,2,12,0,3): 1.2625,
        },
    }

    df = client.obtain_df(2024, 5, 2)
    # df = client.interp_df(df, method = "time")
    # actual_interp = df.to_dict()
    # assert_dicts(actual_interp, expected_interp)
    actual_resample = client.process_data_pandas(df).to_dict()
    assert_dicts(actual_resample, expected_resample)


def test_sparse_column_topics_file(client):
    expected_interp = {
        "pyrometer/ir_01": {
            datetime(2024,5,3,12,0,0,000000): 20.00,
            datetime(2024,5,3,12,0,0,250000): 20.667,
            datetime(2024,5,3,12,0,0,500000): 21.333,
            datetime(2024,5,3,12,0,0,750000): 22.000,
            datetime(2024,5,3,12,0,1,000000): 22.667,
            datetime(2024,5,3,12,0,1,250000): np.nan,
            datetime(2024,5,3,12,0,1,500000): np.nan,
            datetime(2024,5,3,12,0,1,750000): np.nan,
            datetime(2024,5,3,12,0,2,000000): np.nan,
            datetime(2024,5,3,12,0,2,250000): np.nan,
            datetime(2024,5,3,12,0,2,500000): np.nan,
            datetime(2024,5,3,12,0,2,750000): np.nan,
            datetime(2024,5,3,12,0,3,000000): np.nan,
            datetime(2024,5,3,12,0,3,250000): np.nan,
            datetime(2024,5,3,12,0,3,500000): np.nan,
            datetime(2024,5,3,12,0,3,750000): 30.00,
        },
        "pressure/stinger": {
            datetime(2024,5,3,12,0,0,000000): np.nan,
            datetime(2024,5,3,12,0,0,250000): 2.00,
            datetime(2024,5,3,12,0,0,500000): 2.00,
            datetime(2024,5,3,12,0,0,750000): 2.00,
            datetime(2024,5,3,12,0,1,000000): 2.00,
            datetime(2024,5,3,12,0,1,250000): 2.00,
            datetime(2024,5,3,12,0,1,500000): 2.00,
            datetime(2024,5,3,12,0,1,750000): 2.00,
            datetime(2024,5,3,12,0,2,000000): 2.00,
            datetime(2024,5,3,12,0,2,250000): 2.00,
            datetime(2024,5,3,12,0,2,500000): 2.00,
            datetime(2024,5,3,12,0,2,750000): 2.00,
            datetime(2024,5,3,12,0,3,000000): 2.00,
            datetime(2024,5,3,12,0,3,250000): 2.00,
            datetime(2024,5,3,12,0,3,500000): 2.00,
            datetime(2024,5,3,12,0,3,750000): 2.00,
        },
    }
    expected_resample = {
        "pyrometer/ir_01": {
            datetime(2024,5,3,12,0,0,000000): 21.00,
            datetime(2024,5,3,12,0,1,000000): 22.667,
            datetime(2024,5,3,12,0,2,000000): np.nan,
            datetime(2024,5,3,12,0,3,000000): 30.00,
        },
        "pressure/stinger": {
            datetime(2024,5,3,12,0,0,000000): 2.0,
            datetime(2024,5,3,12,0,1,000000): 2.0,
            datetime(2024,5,3,12,0,2,000000): 2.0,
            datetime(2024,5,3,12,0,3,000000): 2.0,
        },
    }
    df = client.obtain_df(2024, 5, 3)
    # df = client.interp_df(df)
    # actual_interp = df.to_dict()
    # assert_dicts(actual_interp, expected_interp)
    actual_resample = client.process_data_pandas(df).to_dict()
    assert_dicts(actual_resample, expected_resample)
 

def test_remove_nan_rows(client):
    expected_interp = {
        "pyrometer/ir_01": {
            datetime(2024,5,4,12,0,0,000000): 20.0,
            datetime(2024,5,4,12,0,0,500000): 20.5,
            datetime(2024,5,4,12,0,10,000000): 30.0,
            datetime(2024,5,4,12,0,10,500000): 30.0,
        },
        "pressure/stinger": {
            datetime(2024,5,4,12,0,0,000000): np.nan,
            datetime(2024,5,4,12,0,0,500000): 2.0,
            datetime(2024,5,4,12,0,10,000000): 3.9,
            datetime(2024,5,4,12,0,10,500000): 4.0,
        },
    }
    expected_resample = {
        "pyrometer/ir_01": {
            datetime(2024,5,4,12,0,0,000000): 20.25,
            datetime(2024,5,4,12,0,10,000000): 30.0,
        },
        "pressure/stinger": {
            datetime(2024,5,4,12,0,0,000000): 2.0,
            datetime(2024,5,4,12,0,10,000000): 3.95,
        },
    }
    df = client.obtain_df(2024, 5, 4)
    # df = client.interp_df(df)
    # actual_interp = df.to_dict()
    # assert_dicts(actual_interp, expected_interp)
    actual_resample = client.process_data_pandas(df).to_dict()
    assert_dicts(actual_resample, expected_resample)


def test_empty_topics_files(client):
    df = client.obtain_df(2024,5,5)
    assert df.size == 0
    df = client.obtain_df(2024,5,6)
    assert df is None


def test_single_topic_entry(client):
    expected_interp = {
        "pyrometer/ir_01": {
            datetime(2024,5,3,12,0,0,000000): np.nan,
            datetime(2024,5,3,12,0,0,250000): np.nan,
            datetime(2024,5,3,12,0,0,500000): np.nan,
            datetime(2024,5,3,12,0,0,750000): np.nan,
            datetime(2024,5,3,12,0,1,000000): np.nan,
            datetime(2024,5,3,12,0,1,250000): np.nan,
            datetime(2024,5,3,12,0,1,500000): np.nan,
            datetime(2024,5,3,12,0,1,750000): np.nan,
            datetime(2024,5,3,12,0,2,000000): 20.0,
            datetime(2024,5,3,12,0,2,250000): 20.0,
            datetime(2024,5,3,12,0,2,500000): 20.0,
            datetime(2024,5,3,12,0,2,750000): 20.0,
            datetime(2024,5,3,12,0,3,000000): 20.0,
            datetime(2024,5,3,12,0,3,250000): np.nan,
            datetime(2024,5,3,12,0,3,500000): np.nan,
        },
        "pressure/stinger": {
            datetime(2024,5,3,12,0,0,000000): 2.0,
            datetime(2024,5,3,12,0,0,250000): 2.00,
            datetime(2024,5,3,12,0,0,500000): 2.00,
            datetime(2024,5,3,12,0,0,750000): 2.00,
            datetime(2024,5,3,12,0,1,000000): 2.00,
            datetime(2024,5,3,12,0,1,250000): 2.00,
            datetime(2024,5,3,12,0,1,500000): 2.00,
            datetime(2024,5,3,12,0,1,750000): 2.00,
            datetime(2024,5,3,12,0,2,000000): 2.00,
            datetime(2024,5,3,12,0,2,250000): 2.00,
            datetime(2024,5,3,12,0,2,500000): 2.00,
            datetime(2024,5,3,12,0,2,750000): 2.00,
            datetime(2024,5,3,12,0,3,000000): 2.00,
            datetime(2024,5,3,12,0,3,250000): 2.00,
            datetime(2024,5,3,12,0,3,500000): 2.00,
            datetime(2024,5,3,12,0,3,750000): 2.00,
        },
    }
    expected_resample = {
        "pyrometer/ir_01": {
            datetime(2024,5,3,12,0,0,000000): np.nan,
            datetime(2024,5,3,12,0,1,000000): np.nan,
            datetime(2024,5,3,12,0,2,000000): 20.0,
            datetime(2024,5,3,12,0,3,000000): 20.0,
        },
        "pressure/stinger": {
            datetime(2024,5,3,12,0,0,000000): 2.0,
            datetime(2024,5,3,12,0,1,000000): 2.0,
            datetime(2024,5,3,12,0,2,000000): 2.0,
            datetime(2024,5,3,12,0,3,000000): 2.0,
        },
    }
    df = client.obtain_df(2024, 5, 7)
    # df = client.interp_df(df)
    # actual_interp = df.to_dict()
    # assert_dicts(actual_interp, expected_interp)
    actual_resample = client.process_data_pandas(df).to_dict()
    assert_dicts(actual_resample, expected_resample)


def test_handling_string_values(client):
    expected_interp = {
        "sensor/liner_control/inductive_heater_power_output-percentage": {
            datetime(2024,5,29,8,45,46,0): 1.0,
            datetime(2024,5,29,8,45,46,250000): 1.0,
            datetime(2024,5,29,8,45,46,500000): 1.0,
            datetime(2024,5,29,8,45,46,750000): 1.0,
            datetime(2024,5,29,8,45,47,0): 1.0,
            datetime(2024,5,29,8,45,47,250000): 1.0,
            datetime(2024,5,29,8,45,47,500000): 1.0,
            datetime(2024,5,29,8,45,47,750000): 1.0
        },
        "control/liner_control/inductive_heater_feedback-centigrade": {
            datetime(2024,5,29,8,45,46,0): np.nan,
            datetime(2024,5,29,8,45,46,250000): 1.0,
            datetime(2024,5,29,8,45,46,500000): 1.25,
            datetime(2024,5,29,8,45,46,750000): 1.5,
            datetime(2024,5,29,8,45,47,0): 1.75,
            datetime(2024,5,29,8,45,47,250000): 2.0,
            datetime(2024,5,29,8,45,47,500000): 2.0,
            datetime(2024,5,29,8,45,47,750000): 2.0
        },
        "control/liner_control/inductive_heater_setpoint-centigrade": {
            datetime(2024,5,29,8,45,46,0): np.nan,
            datetime(2024,5,29,8,45,46,250000): np.nan,
            datetime(2024,5,29,8,45,46,500000): 1.0,
            datetime(2024,5,29,8,45,46,750000): 1.5,
            datetime(2024,5,29,8,45,47,0): 2.0,
            datetime(2024,5,29,8,45,47,250000): 2.5,
            datetime(2024,5,29,8,45,47,500000): 3.0,
            datetime(2024,5,29,8,45,47,750000): 3.0
        },
        "control/liner_control/inductive_heater_enable-bool": {
            datetime(2024,5,29,8,45,46,0): np.nan,
            datetime(2024,5,29,8,45,46,250000): np.nan,
            datetime(2024,5,29,8,45,46,500000): np.nan,
            datetime(2024,5,29,8,45,46,750000): "ON",
            datetime(2024,5,29,8,45,47,0): np.nan,
            datetime(2024,5,29,8,45,47,250000): np.nan,
            datetime(2024,5,29,8,45,47,500000): np.nan,
            datetime(2024,5,29,8,45,47,750000): "OFF"
        }
    }
    expected_resample = {
        "sensor/liner_control/inductive_heater_power_output-percentage": {
            datetime(2024,5,29,8,45,46): 1.0,
            datetime(2024,5,29,8,45,47): 1.0,
        },
        "control/liner_control/inductive_heater_feedback-centigrade": {
            datetime(2024,5,29,8,45,46): 1.25,
            datetime(2024,5,29,8,45,47): 1.9375,
        },
        "control/liner_control/inductive_heater_setpoint-centigrade": {
            datetime(2024,5,29,8,45,46): 1.25,
            datetime(2024,5,29,8,45,47): 2.625,
        },
        "control/liner_control/inductive_heater_enable-bool": {
            datetime(2024,5,29,8,45,46): "ON",
            datetime(2024,5,29,8,45,47): "OFF",
        }
    }
    df = client.obtain_df(2024, 5, 29)
    actual_resample = client.process_data_pandas(df).to_dict()
    assert_dicts(actual_resample, expected_resample)


def test_processed_csv_output(client):
    client.end_of_day(2024,5,29)
    

#-------------------Old Unused functions-----------------------------
def test_averaging_list():
    client = DataExtractionClient()
    
    microseconds_array = np.linspace(0, 750, 10)
    date_list = []
    values_list = []
    test_buffer = []
    for microsecond in microseconds_array:
        date_element = datetime(2024, 4, 29, 15, 45, 0, int(microsecond))
        value_element = microsecond
        date_list.append(date_element)
        values_list.append(value_element)  # Random data
        datum = {
            "time": date_element,
            "field": value_element
        }
        test_buffer.append(datum)

    averaged_time = pd.to_datetime(pd.Series(date_list)).mean()
    averaged_values = sum(values_list)/len(values_list)
    actual_average = {
        "time": averaged_time,
        "field": averaged_values
    }

    function_average = client.avg_buffer_interval(test_buffer)
    assert function_average == actual_average


def test_setting_time_interval_lists():
    client = DataExtractionClient()
    
    x = np.linspace(0, 750, 10)
    date_list = []
    values_list = []
    test_buffer: list[dict] = []
    for second in range(0, 10):
        for microsecond in x:
            date_element = datetime(2024, 4, 29, 15, 45, second, int(microsecond))
            value_element = second + microsecond
            date_list.append(date_element)
            values_list.append(value_element)  # Random data
            datum = {
                "time": date_element,
                "field": value_element
            }
            test_buffer.append(datum)
        
    result_list = client.avg_list_by_time_interval(test_buffer, interval_in_seconds = 1)
    assert len(result_list) == 10


def test_interpolate_df():
    sample_data = {
        "time": np.array(
            [datetime(2024, 4, 30, 9, 30, 0),
            datetime(2024, 4, 30, 9, 30, 1),
            datetime(2024, 4, 30, 9, 30, 2),
            datetime(2024, 4, 30, 9, 30, 3),
            datetime(2024, 4, 30, 9, 30, 4)]
        ),
        "temperature": np.array(
            [22.0, 22.5, np.nan, np.nan, 30.0]
        ),
        "pressure": np.array(
            [101.0, 102.0, 103.0, 105.0, np.nan]
        )
    }
    sample_df = pd.DataFrame(sample_data)

    intended_result_data = {
        "time": np.array(
            [datetime(2024, 4, 30, 9, 30, 0),
            datetime(2024, 4, 30, 9, 30, 1),
            datetime(2024, 4, 30, 9, 30, 2),
            datetime(2024, 4, 30, 9, 30, 3),
            datetime(2024, 4, 30, 9, 30, 4)]
        ),
        "temperature": np.array(
            [22.0, 22.5, 25.0, 27.5, 30.0]
        ),
        "pressure": np.array(
            [101.0, 102.0, 103.0, 105.0, 105.0]
        )
    }
    intended_result_df = pd.DataFrame(intended_result_data)
    
    pd.testing.assert_frame_equal(sample_df.interpolate(), intended_result_df)