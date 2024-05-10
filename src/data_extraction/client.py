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
from mqtt_node_network.client import (MQTTClient, MQTTBrokerConfig)
from mqtt_node_network.initialize import initialize
from paho.mqtt.client import MQTTMessage
from buffered.buffer import Buffer

import time
from datetime import datetime
import pandas as pd
import os
from threading import (Thread, Lock)
import json
import logging
from collections import deque
from dataclasses import dataclass


config = initialize(
    config="./config/config.toml", secrets=".env", logger="./config/logger.yaml"
)
logger = logging.getLogger("__name__")
BROKER_CONFIG = config["mqtt"]["broker"]


@dataclass
class DataExtractionConfig():
    name: str
    node_id: str
    max_buffer_length: int
    max_buffer_time: float
    subscriptions: str | list[str]
    topic_structure: str
    id_structure: str
    resample_time: float
    nan_limit: int
    output_filename: str
    output_directory: str
    processed_output_filename: str
    processed_output_directory: str

    def __post_init__(self):
        if not isinstance(self.max_buffer_length, int):
            raise TypeError("max_buffer_length needs to be an integer")
        if not isinstance(self.nan_limit, int):
            raise TypeError("nan_limit needs to be an integer")


EXTRACTION_CONFIG = DataExtractionConfig(
    name = config["data_extraction"]["name"],
    node_id = config["data_extraction"]["node_id"],
    max_buffer_length = config["data_extraction"]["max_buffer_length"],
    max_buffer_time = config["data_extraction"]["max_buffer_time"],
    subscriptions = config["data_extraction"]["subscriptions"],
    topic_structure = config["mqtt"]["node_network"]["topic_structure"],
    id_structure = config["data_extraction"]["id_structure"],
    resample_time = config["data_extraction"]["resample_time"],
    nan_limit = config["data_extraction"]["nan_limit"],
    output_filename = config["data_extraction"]["ouput_filename"],
    output_directory = config["data_extraction"]["output_directory"],
    processed_output_filename = config["data_extraction"]["processed_output_filename"],
    processed_output_directory = config["data_extraction"]["processed_output_directory"]
)


class DataExtractionClient(MQTTClient):
    def __init__(
            self,
            broker_config: MQTTBrokerConfig = BROKER_CONFIG,
            data_extraction_config = EXTRACTION_CONFIG,
            logger = None
        ):
        MQTTClient.__init__(
            self,
            broker_config = broker_config,
            name = data_extraction_config.name,
            node_id = data_extraction_config.node_id,
            node_type = None,
            logger = logger,
            buffer = Buffer(maxlen = 10_000),
            topic_structure = data_extraction_config.topic_structure,
        )
        self.start_time = datetime.now()
        self.lock = Lock()
        self.max_buffer = data_extraction_config.max_buffer_length
        self.resample_time_seconds = data_extraction_config.resample_time
        self.buffer_time_interval = data_extraction_config.max_buffer_time
        self.output_filename = data_extraction_config.output_filename
        self.processed_filename = data_extraction_config.processed_output_filename
        self.id_structure = data_extraction_config.id_structure
        self.output_directory = data_extraction_config.output_directory
        self.processed_directory = data_extraction_config.processed_output_directory
        self.nan_limit = data_extraction_config.nan_limit
        # self.subscribe(data_extraction_config.subscriptions)
        self.sub_test = data_extraction_config.subscriptions

        os.makedirs(self.output_directory, exist_ok=True)
        os.makedirs(self.processed_directory, exist_ok=True)


    def check_message_value(self, message: MQTTMessage):
        if message.payload is None:
            logger.debug(
                f"Null message ignored. Received None on topic '{message.topic}'"
            )
            return

        elif isinstance(message.payload, bytes):
            value = message.payload.decode()

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
        
        return value


    def on_message(self, client, userdata, message: MQTTMessage):
        MQTTNode.on_message(self, client, userdata, message)

        value = self.check_message_value(message)
        if value is None:
            return
        
        # Maybe call parse_topic function from parent class
        message_topic_breakdown = message.topic.split("/")
        topic_structure_breakdown = self.topic_structure.split("/")
        id_breakdown = self.id_structure.split("/")
        topic_template: dict = {}

        for (topic_subsection, message_subsection) in zip(topic_structure_breakdown, message_topic_breakdown):
            topic_template.update({topic_subsection: message_subsection})

        field_id = ""
        for (index, id_subsection) in enumerate(id_breakdown):
            field_id += topic_template.get(id_subsection)
            if index != (len(id_breakdown) - 1):
                field_id += "/"

        data = {
            "time": datetime.now(),
            "topic": message.topic,
            "id": field_id,
            "value": value
        }
        self.buffer.append(data)


    def connect(self):
        MQTTClient.connect(self)
        self.subscribe(self.sub_test)


    def run(self) -> None:
        eod_handle = Thread(target = self.end_of_day_thread)
        buffer_handle = Thread(target = self.manage_buffer_thread)
        eod_handle.start()
        buffer_handle.start()
        eod_handle.join()
        buffer_handle.join()


    #-------------Functions for creating final csv based off of csv full of topics-------------------------------
    def end_of_day_thread(self):
        while True:
            if datetime.now().day != self.start_time.day:
                self.end_of_day(
                    self.start_time.year,
                    self.start_time.month,
                    self.start_time.day
                )
            time.sleep(60)


    def obtain_df(self, year, month, day) -> pd.DataFrame | None:
        try:
            df = pd.read_csv(f"{self.output_directory}/{self.output_filename}_{year}_{month}_{day}.csv")
        except FileNotFoundError:
            logger.info(f"{self.output_filename} file not found. No data to process.")
            return None
        except pd.errors.EmptyDataError as error:
            logger.info(f"Error reading {self.output_filename}: {error}")
            return None
        else:
            return df


    def end_of_day(self, year, month, day) -> None:
        df = self.obtain_df(year, month, day)
        if df is None:
            return
        df = self.process_data_pandas(df)
        os.makedirs(self.processed_directory, exist_ok=True)
        filename = f"{self.processed_directory}/{self.processed_filename}_{year}_{month}_{day}.csv"
        self.start_time = datetime.now()
        self.write_to_file(df, filename)


    def interp_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df["time"] = pd.to_datetime(df["time"])
        df = df.drop(columns = "topic")
        df = df.pivot(index = "time", columns = ["id"], values = "value")
        df = df.interpolate(method = "time", limit = self.nan_limit)
        return df
    
    
    def resample_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.resample(rule = f"{self.resample_time_seconds}s").mean()
        df = df.dropna(axis = 0, how = "all")
        return df


    # Look into this for interpolating with conditions:
    #   https://stackoverflow.com/questions/69951782/pandas-interpolate-with-condition
    def process_data_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        start = time.perf_counter()
        df = self.interp_df(df)
        df = self.resample_df(df)
        stop = time.perf_counter()
        performance_time = stop - start
        logger.info(f"Pandas performance time: {performance_time}")
        return df


    #--------------Functions for updating csv with topics when buffer fills up-----------------------------------
    def manage_buffer_thread(self):
        compare_time = datetime.now()
        while True:
            buffer_time_exceeded = (datetime.now() - compare_time).total_seconds() > self.buffer_time_interval
            if (self.buffer.size() > self.max_buffer) or (buffer_time_exceeded and self.buffer.not_empty()):
                self.dump_buffer_to_csv()
                compare_time = datetime.now()
            time.sleep(1)

    
    def dump_buffer_to_csv(self):
        # update_list = [self.buffer.popleft() for i in range(len(self.buffer))]
        os.makedirs(self.output_directory, exist_ok=True)
        filename = f"{self.output_directory}/{self.output_filename}_{self.start_time.year}_{self.start_time.month}_{self.start_time.day}.csv"
        self.update_csv(self.buffer.dump(), filename)

    #---------------Write to csv functions-----------------------------------------------------------------------
    def update_csv(self, update_list: deque[dict], filename: str) -> None:
        df = pd.DataFrame(update_list)
        self.write_to_file(df, filename, append = True)


    def write_to_file(
            self,
            df: pd.DataFrame,
            filename: str,
            append: bool = False
        ) -> None:
        output_path = filename

        if append:
            df.to_csv(output_path, mode = 'a', header = not os.path.exists(output_path), index = False)
        else:
            df.to_csv(output_path)


    #-----------------------Currently unused---------------------------------------------------------------------
    def avg_list_by_time_interval(self, buffer: deque[dict], interval_in_seconds: int) -> deque[dict]:
        averaged_list = []
        start_index = 0
        for (index, data) in enumerate(buffer):
            if index == 0:
                start_time = data["time"]
                continue
            elif (data["time"] - start_time).total_seconds() > interval_in_seconds:
                averaged_list.append(self.avg_buffer_interval(buffer[start_index:index]))
                start_index = index
                start_time = data["time"]
                print(index)        # DOUBLE CHECK THAT THIS IS WORKING
        if start_index != len(buffer):
            averaged_list.append(self.avg_buffer_interval(buffer[start_index:len(buffer)]))
        return averaged_list


    def avg_buffer_interval(self, time_interval_set_buffer: deque[dict]) -> dict:
        averaged_results = {
            "time": []
        }
        for data in time_interval_set_buffer:
            for (k, v) in data.items():
                if k in averaged_results:
                    averaged_results[k].append(v)
                else:
                    averaged_results.update({k: [v]})
        
        for (k, v_list) in averaged_results.items():
            if k == "time":
                averaged_results.update({k: pd.to_datetime(pd.Series(v_list)).mean()})
            else:
                averaged_results.update({k: sum(v_list)/len(v_list)})
        return averaged_results


    def process_data_manually(self, buffer: deque[dict]) -> pd.DataFrame:
        df = pd.DataFrame(buffer)
        print(df)
        buffer = self.avg_list_by_time_interval(buffer, self.resample_time_seconds)
        df = pd.DataFrame(buffer)
        print(df)
        return df
