import pytest
from data_extraction.client import DataExtractionClient
from mqtt_node_network.client import MQTTBrokerConfig

BROKER_CONFIG = MQTTBrokerConfig(
    username = "test_user",
    password = "test_password",
    keepalive = 60,
    hostname = "test_host",
    port = 1883,
    timeout = 1,
    reconnect_attempts = 3
)
NAME = "test_name"
NODE_ID = "test_node_id"
NODE_TYPE = "test_node_type"
MAX_BUFFER = 1000
CLIENT = DataExtractionClient(
    broker_config = BROKER_CONFIG,
    name = NAME,
    node_id = NODE_ID,
    node_type = NODE_TYPE,
    max_buffer = MAX_BUFFER
)

def test_client_initialization():  
    assert len(CLIENT.buffer) == 0
    assert CLIENT.name == NAME
    assert CLIENT.node_id == NODE_ID
    assert CLIENT.node_type == NODE_TYPE
    assert CLIENT.max_buffer == MAX_BUFFER

