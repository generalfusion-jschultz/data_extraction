#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_module_description"""
# ---------------------------------------------------------------------------
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import (MQTTClient, MQTTBrokerConfig, Metric)
from paho.mqtt.client import MQTTMessage

from datetime import datetime
import pandas as pd
import polars as pl
import os
import threading
import json
import logging

logger = logging.getLogger("__name__")

class DataExtractionClient(MQTTClient):
    def __init__(
            self,
            broker_config: MQTTBrokerConfig,
            name: str = None,
            node_id: str = None,
            node_type = None,
            max_buffer: int = 1000
        ):
        MQTTClient.__init__(
            self,
            broker_config = broker_config,
            name = name,
            node_id = node_id,
            node_type = node_type,
            logger = None,
            buffer = [],
            topic_structure = "machine/module/measurement/field*"
        )
        self.start_time = datetime.now()
        self.max_buffer = max_buffer


    def on_message(self, client, userdata, message: MQTTMessage):
        MQTTNode.on_message(self, client, userdata, message)
        
        # Maybe call parse_topic function from parent class
        field = message.topic.split("/")[3]

        if message.payload is None:
            logger.debug(
                f"Null message ignored. Received None on topic '{message.topic}'"
            )
            return

        elif isinstance(message.payload, bytes):
            value = message.payload.decode()

        if value == "nan":
            logger.debug(
                f"Null message ignored. Received 'nan' on topic '{message.topic}'"
            )
            return
        
        try:
            value = float(value)
        except ValueError:
            pass

        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                logger.debug("Message is not JSON. Attempting to parse as a string")
                pass

        if not isinstance(value, (str, int, float)):
            logger.error(
                f"Message is not a valid type. Received '{type(value)}' on topic '{message.topic}'"
            )
            return

        data = {
            "time": datetime.now(),
            field: value
        }
        self.buffer.append(data)


    def end_of_day(self, buffer: list[dict]) -> None:
        self.start_time = datetime.now()
        self.update_csv(buffer)

    
    def update_csv(self, buffer: list[dict]) -> None:
        processed_data = self.process_data(buffer)
        self.write_to_file(processed_data)


    def avg_by_time_interval(self, buffer: list[dict], interval_in_seconds: int = 1) -> list[dict]:
        averaged_list = []
        for (index, data) in enumerate(buffer):
            if index == 0:
                start_index = index
                start_time = data["time"]
                continue
            elif (data["time"] - start_time).total_seconds() > interval_in_seconds:
                averaged_list.append(self.avg_test(buffer[start_index:index]))
                start_index = index
                start_time = data["time"]
                print(index)        # DOUBLE CHECK THAT THIS IS WORKING
        if start_index != len(buffer):
            averaged_list.append(self.avg_test(buffer[start_index:len(buffer)]))
        return averaged_list

    def avg_test(self, time_interval_set_buffer: list[dict]) -> dict:
        test = {
            "time": []
        }
        for data in time_interval_set_buffer:
            for (k, v) in data.items():
                if k in test:
                    test[k].append(v)
                else:
                    test.update({k: [v]})
        
        for (k, v_list) in test.items():
            if k == "time":
                test.update({k: pd.to_datetime(pd.Series(v_list)).mean()})
            else:
                test.update({k: sum(v_list)/len(v_list)})
        return test


    def process_data(self, buffer: list[dict]) -> pd.DataFrame:
        # data = []
        # for metric in buffer:
        #     datum = {}
        #     datum.update({"time": metric.time})
        #     for k, v in metric.fields.items():
        #         datum.update({k: v})
        #     data.append(datum)
        
        df = pd.DataFrame(buffer)
        print(df)
        buffer = self.avg_by_time_interval(buffer, 2)
        df = pd.DataFrame(buffer)
        print(df)
        return df


    def write_to_file(self, df: pd.DataFrame):
        year = self.start_time.year
        month = self.start_time.month
        day = self.start_time.day
        path = "./output/"
        os.makedirs(path, exist_ok = True)
        filename = f"temp_&_pressure_data_{year}_{month}_{day}.csv"
        df.to_csv(path + filename)


    def run(self):
        while (datetime.now() - self.start_time).total_seconds() < 10:
        # while True:
            current_hour = datetime.now().hour
            current_day = datetime.now().day
            if (current_hour == 0) and (current_day != self.start_time.day):
                full_buffer = self.buffer
                self.buffer.clear()
                day_end_thread = threading.Thread(target = self.end_of_day(full_buffer))
                day_end_thread.start()
            if len(self.buffer) > self.max_buffer:
                full_buffer = self.buffer
                self.buffer.clear()
                update_file_thread = threading.Thread(self.update_csv())
                update_file_thread.start()
        self.process_data(self.buffer)