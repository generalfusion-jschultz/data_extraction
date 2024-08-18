#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Jason Schultz
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""MQTT client for backing up subscribed topics to csv"""
# ---------------------------------------------------------------------------
from mqtt_node_network.node import MQTTNode
from mqtt_node_network.client import (MQTTClient, MQTTBrokerConfig)
from mqtt_node_network.initialize import initialize
from paho.mqtt.client import MQTTMessage
from buffered.buffer import Buffer
from prometheus_client import Gauge
import numpy as np

import time
from datetime import datetime
import pandas as pd
import os
from threading import Thread
import json
import logging
from collections import deque
from dataclasses import dataclass
import psutil

logger = logging.getLogger("data_extraction")

config = initialize(
    config="./config/config.toml", secrets=".env", logger="./config/logger.yaml"
)
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
            raise TypeError("max_buffer_length needs to be an integer.")
        if not isinstance(self.nan_limit, int):
            raise TypeError("nan_limit needs to be an integer.")
        if not isinstance(self.topic_structure, str):
            raise TypeError("topic_structure needs to be a string.")
        if not isinstance(self.id_structure, str):
            raise TypeError("id_structure needs to be a string.")
        if not isinstance(self.output_directory, str):
            raise TypeError("output_directory needs to be a string.")
        if not isinstance(self.output_filename, str):
            raise TypeError("output_filename needs to be a string.")
        if not isinstance(self.processed_output_directory, str):
            raise TypeError("processed_output_directory needs to be a string.")
        if not isinstance(self.processed_output_filename, str):
            raise TypeError("processed_output_filename needs to be a string.")
        if not isinstance(self.resample_time, (int, float)):
            raise TypeError("resample_time needs to be either an integer or float.")
        if not isinstance(self.subscriptions, str):
            if isinstance(self.subscriptions, list):
                for subscription in self.subscriptions:
                    if not isinstance(subscription, str):
                        raise TypeError(f"Subscription: {subscription} is not a string.")
            else:
                raise TypeError("subscriptions need to be either a string or list of strings")


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
    client_memory_usage = Gauge(
        "client_memory_usage",
        "Program's memory usage in MB",
    )
    client_buffer_length = Gauge(
        "client_buffer_length",
        "Length of the buffer before dumping",
    )
    client_df_performance_time = Gauge(
        "client_dataframe_performance_time",
        "Time in seconds to process csv file in Pandas",
    )
    
    def __init__(
            self,
            broker_config: MQTTBrokerConfig = None,
            data_extraction_config: DataExtractionConfig = None,
        ):
        broker_config = broker_config or BROKER_CONFIG
        data_extraction_config = data_extraction_config or EXTRACTION_CONFIG
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
        self.max_buffer = data_extraction_config.max_buffer_length
        self.resample_time_seconds = data_extraction_config.resample_time
        self.buffer_time_interval = data_extraction_config.max_buffer_time
        self.output_filename = data_extraction_config.output_filename
        self.processed_filename = data_extraction_config.processed_output_filename
        self.id_structure = data_extraction_config.id_structure
        self.output_directory = data_extraction_config.output_directory
        self.processed_directory = data_extraction_config.processed_output_directory
        self.nan_limit = data_extraction_config.nan_limit
        self.subscriptions = data_extraction_config.subscriptions

        # Initializing threads
        self.eod_handle = Thread(target = self.end_of_day_thread)
        self.buffer_handle = Thread(target = self.manage_buffer_thread)
        self.performance_handle = Thread(target = self.performance_thread)
        self.continue_flag: bool = True

        # os.makedirs(self.output_directory, exist_ok=True)
        # os.makedirs(self.processed_directory, exist_ok=True)

        self.metrics_label_value = ""
        for (index, subscription) in enumerate(self.subscriptions):
            if index != 0:
                self.metrics_label_value += " + "
            self.metrics_label_value += subscription.split("/")[0]

#-------------------Functions for performance monitoring------------------------------------------------------
    def performance_thread(self) -> None:
        while self.continue_flag:
            time.sleep(10)
            self.performace()


    # https://psutil.readthedocs.io/en/latest/
    # https://stackoverflow.com/questions/276052/how-to-get-current-cpu-and-ram-usage-in-python
    def performace(self) -> None:
        svmem = psutil.virtual_memory()
        if svmem.percent >= 95:
            logger.critical(f"VM memory at {svmem.percent} %! Shutting down program.")
            self.continue_flag = False
        python_process = psutil.Process(os.getpid())
        memory_usage = python_process.memory_info().rss / (1024*1024)  # MB
        logger.debug(f"Memory usage: {memory_usage:.2f} MB")
        # self.client_memory_usage.labels(
        #     subscriptions = self.metrics_label_value
        # ).set(memory_usage)
        self.client_memory_usage.set(memory_usage)
        memory_limit = 2*1024
        if memory_usage > memory_limit:
            logger.critical(f"Memory usage of {memory_usage:.2f} MB > {memory_limit} MB. Shutting down program.")
            self.continue_flag = False


#-------------------General operational functions-------------------------------------------------------------
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


    def on_message(self, client, userdata, message: MQTTMessage) -> None:
        MQTTNode.on_message(self, client, userdata, message)

        value = self.check_message_value(message)
        if value is None:
            return
        
        # Maybe call parse_topic function from parent class
        message_topic_breakdown = message.topic.split("/")
        topic_structure_breakdown = self.topic_structure.split("/")
        id_breakdown = self.id_structure.split("/")
        topic_template: dict = {}

        try:
            assert len(message_topic_breakdown) == len(topic_structure_breakdown)
        except AssertionError:
            logger.warning("Topic structure and message structure lengths not equal. Check config file.")

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


    def run(self) -> None:
        self.eod_handle.start()
        self.buffer_handle.start()
        self.performance_handle.start()
        logger.info("Threads started.")


    def run_forever(self) -> None:
        self.run()
        while self.continue_flag:
            time.sleep(1)
        self.stop()

    
    def stop(self) -> None:
        logger.info("Process stopping")
        self.continue_flag = False
        if self.buffer_handle.is_alive():
            self.buffer_handle.join()
        if self.performance_handle.is_alive():
            self.performance_handle.join()
        if self.eod_handle.is_alive():
            self.eod_handle.join()


    #-------------Functions for creating final csv based off of csv full of topics-------------------------------
    def end_of_day_thread(self) -> None:
        while self.continue_flag:
            if datetime.now().day != self.start_time.day:
                logger.info("EOD reached")
                self.end_of_day(
                    self.start_time.year,
                    self.start_time.month,
                    self.start_time.day
                )
            time.sleep(15)


    def obtain_df(self, year, month, day) -> pd.DataFrame | None:
        filename = f"{self.output_directory}/{year}{month:02d}{day:02d}-{self.output_filename}.csv"
        try:
            df = pd.read_csv(filename, usecols = [0, 2, 3])
        except FileNotFoundError:
            logger.info(f"{self.output_filename} file not found. No data to process.")
            return None
        except pd.errors.EmptyDataError as error:
            logger.error(f"Error reading {self.output_filename}: {error}")
            return None
        else:
            return df


    def end_of_day(self, year, month, day) -> None:
        self.start_time = datetime.now()
        df = self.obtain_df(year, month, day)
        if df is None:
            return
        df = self.process_data_pandas(df)
        os.makedirs(self.processed_directory, exist_ok=True)
        filename = f"{self.processed_directory}/{year}{month:02d}{day:02d}-{self.processed_filename}.csv"
        self.write_to_file(df, filename)


    def interp_df(self, df: pd.DataFrame, method: str) -> pd.DataFrame:
        df = df.pivot(index = "time", columns = ["id"], values = "value")
        df = df.interpolate(method = method, limit = self.nan_limit)
        return df
    
    
    def resample_df(self, df: pd.DataFrame, how: str) -> pd.DataFrame:
        df = df.resample(rule = f"{self.resample_time_seconds}s").last()
        df = df.dropna(axis = 0, how = "all")
        return df


    def can_cast_to_float(self, series: pd.Series) -> bool:
        try:
            # Try to cast each value to float
            series.astype(float)
            return True
        except ValueError:
            return False


    def split_df_by_float(self, df: pd.DataFrame):
        float_columns = []
        string_columns = []

        # Iterate over each column
        for col in df.columns:
            if self.can_cast_to_float(df[col]):
                float_columns.append(col)
            else:
                string_columns.append(col)

        # Create DataFrames with selected columns
        float_df = df[float_columns].astype("float64")
        string_df = df[string_columns]

        return float_df, string_df

    # ---------------- Using generators for memory efficient parsing of csv data-----------------------------
    def get_unique_ids(self, filename: str) -> list[str]:
        id_list = []
        with open(filename, "r") as file:
            next(file, None)  # CHECK FOR ERROR CONDITION IF ONLY 1 ROW EXISTS?
            for row in file:
                row_id = row.split(",")[2]
                if row_id not in id_list:
                    id_list.append(row_id)
        return id_list


    def yield_row_from_csv(self, filename: str):
        with open(filename, "r") as file:
            next(file, None)  # CHECK FOR ERROR CONDITION IF ONLY 1 ROW EXISTS?
            for row in file:
                row = row.split(",")
                row_time = row[0]
                row_id = row[2]
                row_value = row[3]
                yield (row_time, row_id, row_value)

    
    def manage_csv_buffer(self, filename: str):
        id_list = self.get_unique_ids(filename)
        data: list = []

        generator = self.yield_row_from_csv(filename)
        for _ in range(self.max_buffer):
            (row_time, row_id, row_value) = next(generator)
            row = {"time": row_time}
            for unique_id in id_list:
                if unique_id is row_id:
                    row.update({row_id: row_value})
                else:
                    row.update({unique_id: None})
            data.append(row)
            

    # Look into this for interpolating with conditions:
    #   https://stackoverflow.com/questions/69951782/pandas-interpolate-with-condition
    def process_data_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting to process data")
        start = time.perf_counter()
        
        df["time"] = pd.to_datetime(df["time"])
        df = df.pivot(index = "time", columns = ["id"], values = "value")
        float_df, string_df = self.split_df_by_float(df)

        float_df = float_df.interpolate(method = "time", limit = self.nan_limit)
        float_df = float_df.resample(rule = f"{self.resample_time_seconds}s").mean()
        string_df = string_df.resample(rule = f"{self.resample_time_seconds}s").last()
        result = pd.concat([float_df, string_df], axis = 1)
        result = result.dropna(axis = 0, how = "all")

        stop = time.perf_counter()
        performance_time = stop - start
        self.client_df_performance_time.set(performance_time)
        logger.info(f"Pandas performance time: {performance_time:.2f} s")
        return result


    #--------------Functions for updating csv with topics when buffer fills up-----------------------------------
    def manage_buffer_thread(self) -> None:
        compare_time = datetime.now()
        while self.continue_flag:
            buffer_time_exceeded = (datetime.now() - compare_time).total_seconds() > self.buffer_time_interval
            if (self.buffer.size() > self.max_buffer) or (buffer_time_exceeded and self.buffer.not_empty()):
                self.client_buffer_length.set(self.buffer.size())
                self.dump_buffer_to_csv()
                compare_time = datetime.now()
            time.sleep(1)

    
    def dump_buffer_to_csv(self) -> None:
        # update_list = [self.buffer.popleft() for i in range(len(self.buffer))]
        os.makedirs(self.output_directory, exist_ok=True)
        filename = f"{self.output_directory}/{self.start_time.year}{self.start_time.month:02d}{self.start_time.day:02d}-{self.output_filename}.csv"
        self.update_csv(self.buffer.dump(), filename)

    #---------------Write to csv functions-----------------------------------------------------------------------
    def update_csv(self, update_list: deque[dict], filename: str) -> None:
        df = pd.DataFrame(update_list)
        self.write_to_file(df, filename, append = True)


    def write_to_file(
            self,
            df: pd.DataFrame,
            filepath: str,
            append: bool = False
        ) -> None:

        if append:
            df.to_csv(filepath, mode = 'a', header = not os.path.exists(filepath), index = False)
        else:
            df.to_csv(filepath)


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


    def __del__(self):
        self.stop()