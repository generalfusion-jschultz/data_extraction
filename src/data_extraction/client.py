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
from mqtt_node_network.configure import load_config
from paho.mqtt.client import MQTTMessage

import time
from datetime import datetime
import pandas as pd
import polars as pl
import os
from threading import (Thread, Lock)
import json
import logging
from collections import deque


config = initialize(
    config="./config/config.toml", secrets=".env", logger="./config/logger.yaml"
)
BROKER_CONFIG = config["mqtt"]["broker"]
TOPIC_STRUCTURE = config["mqtt"]["node_network"]["topic_structure"]

CLIENT_NAME = config["data_extraction"]["name"]
CLIENT_NODE_ID = config["data_extraction"]["node_id"]
ID_STRUCTURE = config["data_extraction"]["id_structure"]
MAX_BUFFER_LENGTH = config["data_extraction"]["max_buffer_length"]
MAX_BUFFER_TIME = config["data_extraction"]["max_buffer_time"]
RESAMPLE_TIME = config["data_extraction"]["resample_time"]
RAW_OUTPUT_FILENAME = config["data_extraction"]["raw_ouput_filename"]
PROCESSED_FILENAME = config["data_extraction"]["processed_output_filename"]
SUBSCRIPTIONS = config["data_extraction"]["subscriptions"]

logger = logging.getLogger("__name__")

class DataExtractionClient(MQTTClient):
    def __init__(
            self,
            broker_config: MQTTBrokerConfig = BROKER_CONFIG,
            name: str = CLIENT_NAME,
            node_id: str = CLIENT_NODE_ID,
            node_type = None,
            max_buffer: int = MAX_BUFFER_LENGTH,
            resample_time_seconds: float = RESAMPLE_TIME,
            buffer_time_interval: int = MAX_BUFFER_TIME,
            subscriptions: str | list[str] = SUBSCRIPTIONS,  # Currently unused
            topic_structure: str = TOPIC_STRUCTURE,
            raw_output_filename: str = RAW_OUTPUT_FILENAME,
            processed_output_filename: str = PROCESSED_FILENAME
        ):
        MQTTClient.__init__(
            self,
            broker_config = broker_config,
            name = name,
            node_id = node_id,
            node_type = node_type,
            logger = None,
            buffer = deque(),
            topic_structure = topic_structure,
        )
        self.start_time = datetime.now()
        self.lock = Lock()
        self.max_buffer = max_buffer
        self.resample_time_seconds = resample_time_seconds
        self.buffer_time_interval = buffer_time_interval
        self.raw_output_filename = raw_output_filename
        self.processed_output_filename = processed_output_filename


    def on_message(self, client, userdata, message: MQTTMessage):
        MQTTNode.on_message(self, client, userdata, message)
        
        # Maybe call parse_topic function from parent class
        topic = message.topic
        field = message.topic.split("/")[3]
        measurement = message.topic.split("/")[2]
        field_id = measurement + "/" + field

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
            field_id: value
        }
        self.buffer.append(data)


    def run(self) -> None:
        eod_handle = Thread(target = self.end_of_day_thread)
        buffer_handle = Thread(target = self.manage_buffer_thread)

        eod_handle.start()
        buffer_handle.start()

        eod_handle.join()
        buffer_handle.join()

        # while True:
        #     time.sleep(0.05)


        # while (datetime.now() - self.start_time).total_seconds() < 10:
        # while True:
        #     current_hour = datetime.now().hour
        #     current_day = datetime.now().day
        #     if (current_hour == 0) and (current_day != self.start_time.day):
        #         if threading:
        #             day_end_thread = Thread(target = self.end_of_day(self.buffer.copy()))
        #             self.buffer.clear()
        #             day_end_thread.start()
        #         else:
        #             self.end_of_day(self.buffer.copy())
        #             self.buffer.clear()
        #     if len(self.buffer) > self.max_buffer:
        #         if threading:
        #             update_file_thread = Thread(self.update_csv(self.buffer.copy()))
        #             self.buffer.clear()
        #             update_file_thread.start()
        #         else:
        #             self.update_csv(self.buffer.copy())
        #             self.buffer.clear()
        # self.process_data(self.buffer)


    #-------------Functions for creating final csv based off of csv full of topics-------------------------------
    def end_of_day_thread(self):
        while True:
            current_hour = datetime.now().hour
            current_day = datetime.now().day
            if (current_hour == 0) and (current_day != self.start_time.day):
                self.end_of_day(
                    self.start_time.year,
                    self.start_time.month,
                    self.start_time.day
                )
            time.sleep(60)


    def end_of_day(self, year, month, day) -> None:
        df = pd.read_csv(f"./output/{self.raw_output_filename}_{year}_{month}_{day}.csv")
        df["time"] = pd.to_datetime(df["time"])
        df = self.process_data_pandas(df)
        filename = f"./processed_output/{self.processed_output_filename}"
        self.start_time = datetime.now()
        self.write_to_file(df, filename)


    # Look into this for interpolating with conditions:
    #   https://stackoverflow.com/questions/69951782/pandas-interpolate-with-condition
    def process_data_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        start = time.perf_counter()
        df = df.interpolate()
        df = df.resample(rule = f'{self.resample_time_seconds}s', on = "time")
        df = df.mean()
        stop = time.perf_counter()
        performance_time = stop - start
        print(f"Pandas performance time: {performance_time}")
        return df

    # TODO: Results are scuffed with how the group_by_dynamics function works?
    def process_data_polars(self, buffer: deque[dict]) -> pl.DataFrame:
        start = time.perf_counter()
        df = pl.DataFrame(buffer)
        print("Raw Polars df:")
        print(df)
        df = df.interpolate().set_sorted("time")
        df = df.group_by_dynamic("time", every = f"{self.resample_time_seconds}s").agg(pl.exclude("time")).mean()
        stop = time.perf_counter()
        performance_time = stop - start
        print("Final Polars df: ")
        print(df)
        print(f"Polars performance time: {performance_time}")
        return df
    

    #--------------Functions for updating csv with topics when buffer fills up-----------------------------------
    def manage_buffer_thread(self):
        compare_time = datetime.now()
        while True:
            current_buffer_length = len(self.buffer)
            if (current_buffer_length > self.max_buffer) or ((datetime.now() - compare_time).total_seconds() > self.buffer_time_interval):
                update_list = [self.buffer.popleft() for i in range(current_buffer_length)]
                self.update_csv(update_list, filename = f"./output/{self.raw_output_filename}")
                compare_time = datetime.now()

            time.sleep(1)

    
    #---------------Write to csv functions-----------------------------------------------------------------------
    def update_csv(self, update_list: deque[dict], filename: str, use_polars: bool = False) -> None:
        if use_polars:
            df = pl.DataFrame(update_list)
        else:
            df = pd.DataFrame(update_list)
        self.write_to_file(df, filename)


    def write_to_file(self, df: pd.DataFrame | pl.DataFrame, filename: str) -> None:
        year = self.start_time.year
        month = self.start_time.month
        day = self.start_time.day
        output_path = filename + f"_{year}_{month}_{day}.csv"

        if isinstance(df, pd.DataFrame):
            df.to_csv(output_path, mode = 'a', header = not os.path.exists(output_path))
        # TODO: Need to implement appending to the file if it already exists for Polars
        elif isinstance(df, pl.DataFrame):
            df.write_csv(output_path)


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
