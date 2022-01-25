#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from convertCSV import *

def test_conversion():
    # Open Files
    inputFile = pd.read_csv(r'test/input.csv')
    outputFile = pd.read_csv(r'test/output.csv')
    # Convert File to match desire output
    converted_input_file = convert_input(inputFile)
    # Ensure that all the dtypes are correct
    input_csv = ensure_dtype(converted_input_file)
    output_csv = ensure_dtype(outputFile)
    # Check if input matches output
    assert all(input_csv == output_csv)
