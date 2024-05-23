#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Jason Schultz
# Created Date: 2023-04-25
# version ='1.0'
# ---------------------------------------------------------------------------
"""Subscribes to published MQTT nodes and periodically writes them to a csv file."""
# ---------------------------------------------------------------------------
from data_extraction.client import DataExtractionClient
# from data_extraction.client import DataExtractionConfig
# import time
# from datetime import datetime

def main():
    client = DataExtractionClient()
    client.connect()
    client.run_forever()
    # client.run()
    # while True:
    #     time.sleep(30)
    #     client.start_time = datetime(2024,5,22)


if __name__ == "__main__":
    main()
