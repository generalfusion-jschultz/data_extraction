import pytest
from data_extraction.client import DataExtractionClient
from mqtt_node_network.client import MQTTBrokerConfig
from datetime import datetime
import numpy as np
import pandas as pd
from paho.mqtt.client import MQTTMessage

USERNAME = "test_user"
PASSWORD = "test_password"
HOSTNAME = "test_host"
PORT = 1883
BROKER_CONFIG = MQTTBrokerConfig(
    username = USERNAME,
    password = PASSWORD,
    keepalive = 60,
    hostname = HOSTNAME,
    port = PORT,
    timeout = 1,
    reconnect_attempts = 3
)
NAME = "test_name"
NODE_ID = "test_node_id"
ID_STRUCtURE = "measurement/field*"
NODE_TYPE = "test_node_type"
MAX_BUFFER_LENGTH = 1000
MAX_BUFFER_TIME = 10
RESAMPLE_TIME = 1
RAW_OUTPUT_FILENAME = "raw"
PROCESSED_OUTPUT_FILENAME = "processed"


def test_client_initialization():  
    client = DataExtractionClient(
        broker_config = BROKER_CONFIG,
        name = NAME,
        node_id = NODE_ID,
        node_type = NODE_TYPE,
        max_buffer = MAX_BUFFER_LENGTH,
        buffer_time_interval = MAX_BUFFER_TIME,
        resample_time_seconds = RESAMPLE_TIME,
        raw_output_filename = RAW_OUTPUT_FILENAME,
        processed_output_filename = PROCESSED_OUTPUT_FILENAME
    )
    
    assert len(client.buffer) == 0
    assert client.name == NAME
    assert client.node_id == NODE_ID
    assert client.node_type == NODE_TYPE
    assert client.max_buffer == MAX_BUFFER_LENGTH
    assert client._username == USERNAME
    assert client._password == PASSWORD
    assert client.port == PORT
    assert client.buffer_time_interval == MAX_BUFFER_TIME
    assert client.resample_time_seconds == RESAMPLE_TIME
    assert client.raw_output_filename == RAW_OUTPUT_FILENAME
    assert client.processed_output_filename == PROCESSED_OUTPUT_FILENAME


def test_on_message(mocker):
    class MockMessage:
        def __init__(self):
            self.time = datetime.now()
            self.topic = "p0/enclosure/pyrometer/temperature"
            self.id = "pyrometer/temperature"
            self.value = 5
    mocker.patch("mqtt_node_network.node.MQTTNode.on_message", return_value = None)
    
    mock_message = MockMessage()
    mocker.patch("data_extraction.client.DataExtractionClient.check_message_value", return_value = mock_message.value)

    client = DataExtractionClient()
    client.on_message(None, None, mock_message)

    assert len(client.buffer) == 1
    assert (client.buffer[0]["time"] - mock_message.time).total_seconds() == pytest.approx(0, abs = 1)
    assert client.buffer[0]["topic"] == mock_message.topic
    assert client.buffer[0]["id"] == mock_message.id
    assert client.buffer[0]["value"] == mock_message.value


def test_process_df():
    client = DataExtractionClient(
        resample_time_seconds = 2
    )
    df_sample = pd.DataFrame({
        "time": ["2024-05-01 14:17:20",
                 "2024-05-01 14:17:21",
                 "2024-05-01 14:17:22",
                 "2024-05-01 14:17:23"],
        "topic": ["p0/enclosure/pyrometer/temperature",
                  "p0/enclosure/stinger/pressure",
                  "p0/enclosure/pyrometer/temperature",
                  "p0/enclosure/stinger/pressure"],
        "id": ["pyrometer/temperature",
               "stinger/pressure",
               "pyrometer/temperature",
               "stinger/pressure"],
        "value": [20.0, 101.0, 30.0, 102.0]
    })
    df_intended = pd.DataFrame({
        "time": [datetime(2024, 5, 1, 14, 17, 20),
                 datetime(2024, 5, 1, 14, 17, 22)],
        "pyrometer/temperature": [22.5, 30.0],
        "stinger/pressure": [101.0, 101.75]
    }).set_index("time")
    df_actual = client.process_data_pandas(df_sample)
    assert len(df_actual) == 2
    assert df_actual.to_dict() == df_intended.to_dict()


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