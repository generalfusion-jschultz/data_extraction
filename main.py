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
import pandas as pd

# TODO: - Write test conditions
#       - Look into limit condition for interpolating sparse data
#       - May need to lock variables in threads when accessing them
#       - Add checks for output folder directories
#       - Resampling the interpolated data at rates < 1s gives sparse results
#           - Might be due to being lower frequency than publish rate?
#           - Might need to resample first then interpolate?

def main():
    client = DataExtractionClient()
    client.connect()
    client.subscribe(topic = "prototype-zero/#")
    client.run()

    # client.end_of_day(2024, 5, 1)    


if __name__ == "__main__":
    main()
