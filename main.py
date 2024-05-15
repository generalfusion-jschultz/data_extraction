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
from datetime import datetime
# https://github.com/generalmattza/buffered

def main():
    client = DataExtractionClient()
    client.connect()
    # client.start_time = datetime(2024,5,7)
    client.run_forever()


if __name__ == "__main__":
    main()
