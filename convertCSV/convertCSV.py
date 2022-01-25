#!/usr/bin/env python
# coding:utf-8
"""
Python script to convert a input.csv file to a desired output.csv file

Author: Gregor Pfalz
github: GPawi
"""

import pandas as pd
import numpy as np


def convert_input(input_file):
    """
    Convert the original input file into the desired format.
    #Input:
    @input_file: csv file as pandas DataFrame
    #Output:
    @merge_output: transformed pandas DataFrame as desired output
    """
    # Split the input file into the individual nodes from Hestia
    cycle_node = input_file.filter(regex=r'^cycle.')\
        .replace('-', np.nan).dropna(axis=0)
    site_node = input_file.filter(regex=r'^site.')\
        .replace('-', np.nan).dropna(axis=0)
    impact_node = input_file.filter(regex=r'^impactAssessment.')\
        .replace('-', np.nan).dropna(axis=0)
    source_node = input_file.filter(regex=r'^source.')\
        .replace('-', np.nan).dropna(axis=0)
    # Merge nodes on the specific overlapping IDs
    merge_1 = pd.merge(site_node, cycle_node, how='inner',
                       left_on=['site.@id'], right_on=['cycle.site.@id'])
    merge_2 = pd.merge(merge_1, impact_node, how='inner',
                       left_on=['cycle.@id'], right_on=['impactAssessment.cycle.@id'])
    merge_3 = pd.merge(merge_2, source_node, how='inner',
                       left_on=['impactAssessment.source.@id'], right_on=['source.@id'])
    # Sort the output based on cycle.@id
    merge_output = merge_3[input_file.columns].sort_values(by=['cycle.@id'], ignore_index=True)
    # Return converted output
    return merge_output


def ensure_dtype(data):
    """
    To ensure that input file and output file are comparable,
    data types should be the same in all files.
    #Input:
    @data: pandas DataFrame
    #Output:
    @data_trans: transformed pandas DataFrame with corrected dtype
    """
    # dtypes taken from Hestia website
    data_trans = data.astype(dtype={'cycle.@id': str,
                                    'cycle.name': str,
                                    'cycle.description': str,
                                    'cycle.endDate': str,
                                    'cycle.functionalUnit': str,
                                    'impactAssessment.@id': str,
                                    'impactAssessment.name': str,
                                    'impactAssessment.endDate': str,
                                    'impactAssessment.functionalUnitQuantity': int,
                                    'impactAssessment.allocationMethod': str,
                                    'impactAssessment.systemBoundary': bool,
                                    'site.@id': str,
                                    'site.name': str,
                                    'site.siteType': str,
                                    'source.@id': str,
                                    'source.name': str,
                                    'cycle.site.@id': str,
                                    'cycle.defaultSource.@id': str,
                                    'cycle.inputs.0.term.@id': str,
                                    'cycle.inputs.0.term.name': str,
                                    'cycle.inputs.0.value': float,
                                    'cycle.inputs.1.term.@id': str,
                                    'cycle.inputs.1.term.name': str,
                                    'cycle.inputs.1.value': float,
                                    'cycle.inputs.2.term.@id': str,
                                    'cycle.inputs.2.term.name': str,
                                    'cycle.inputs.2.value': float,
                                    'cycle.inputs.3.term.@id': str,
                                    'cycle.inputs.3.term.name': str,
                                    'cycle.inputs.3.value': float,
                                    'cycle.emissions.0.term.@id': str,
                                    'cycle.emissions.0.term.name': str,
                                    'cycle.emissions.0.value': float,
                                    'cycle.emissions.0.methodModel.@id': str,
                                    'cycle.emissions.0.methodModel.name': str,
                                    'cycle.emissions.0.methodTier': str,
                                    'cycle.emissions.1.term.@id': str,
                                    'cycle.emissions.1.term.name': str,
                                    'cycle.emissions.1.value': float,
                                    'cycle.emissions.1.methodModel.@id': str,
                                    'cycle.emissions.1.methodModel.name': str,
                                    'cycle.emissions.1.methodTier': str,
                                    'cycle.emissions.2.term.@id': str,
                                    'cycle.emissions.2.term.name': str,
                                    'cycle.emissions.2.value': float,
                                    'cycle.emissions.2.methodModel.@id': str,
                                    'cycle.emissions.2.methodModel.name': str,
                                    'cycle.emissions.2.methodTier': str,
                                    'cycle.products.0.term.@id': str,
                                    'cycle.products.0.term.name': str,
                                    'cycle.products.0.value': float,
                                    'cycle.products.0.primary': bool,
                                    'impactAssessment.cycle.@id': str,
                                    'impactAssessment.country.@id': str,
                                    'impactAssessment.country.name': str,
                                    'impactAssessment.product.@id': str,
                                    'impactAssessment.product.name': str,
                                    'impactAssessment.source.@id': str,
                                    'site.defaultSource.@id': str,
                                    'site.country.@id': str,
                                    'site.country.name': str,
                                    'site.measurements.0.term.@id': str,
                                    'site.measurements.0.term.name': str,
                                    'site.measurements.0.value': float,
                                    'site.measurements.1.term.@id': str,
                                    'site.measurements.1.term.name': str,
                                    'site.measurements.1.value': float,
                                    'site.measurements.2.term.@id': str,
                                    'site.measurements.2.term.name': str,
                                    'site.measurements.2.value': float,
                                    'site.measurements.3.term.@id': str,
                                    'site.measurements.3.term.name': str,
                                    'site.measurements.3.value': float,
                                    'source.bibliography.name': str,
                                    'source.bibliography.documentDOI': str,
                                    'source.bibliography.title': str})
    # Return transformed DataFrame
    return data_trans
