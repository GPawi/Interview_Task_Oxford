#!/usr/bin/env python
# coding:utf-8
"""
Python script to convert a input.csv file to a desired output.csv file

Author: Gregor Pfalz
github: GPawi
"""

import pandas as pd
import numpy as np

# Expandable dictionary containing filter criteria from Hestia
dict_node_filter = {'cycle': r'^cycle.',
                    'site': r'^site.',
                    'impactAssessment': r'impactAssessment.',
                    'source': r'^source.'}

# Expandable dictionary containing the sequence to merge DataFrames
# based on the specific overlapping IDs
"""
Format  => 'Initial_merge': [[Node_DataFrame1, ID1], [Node_DataFrame2, ID2]]
        => 'Merge_step1': [[Previous_MergeOutput_DataFrame0, ID-M0], [Node_DataFrame3, ID3]]
        => 'Merge_step2': [[Previous_MergeOutput_DataFrame1, ID-M1], [Node_DataFrame4, ID4]]
"""
dict_merge_sequence = {'Initial_merge': [['site', 'site.@id'],
                                         ['cycle', 'cycle.site.@id']],
                       'Merge_1': [['merge_output', 'cycle.@id'],
                                   ['impactAssessment', 'impactAssessment.cycle.@id']],
                       'Merge_2': [['merge_output', 'impactAssessment.source.@id'],
                                   ['source', 'source.@id']]}


def convert_input(input_file):
    """
    Convert the original input file into the desired format.
    #Input:
    @input_file: csv file as pandas DataFrame
    #Output:
    @merge_output: transformed pandas DataFrame as desired output
    """
    # Split the input file into the individual nodes based on filter criteria from Hestia
    dict_node_frame = {}
    for key in dict_node_filter:
        filter_frame = input_file.filter(regex=dict_node_filter[key])\
            .replace('-', np.nan)\
            .dropna(axis=0)\
            .reset_index(drop=True)
        if len(filter_frame.columns) != 0:
            dict_node_frame[key] = filter_frame
    # Merge nodes based on sequential dictionary
    merge_result = {}
    for key in dict_merge_sequence:
        if 'Initial' in key:
            merge_result['merge_output'] = pd.merge(dict_node_frame[dict_merge_sequence[key][0][0]],
                                                    dict_node_frame[dict_merge_sequence[key][1][0]],
                                                    how='inner',
                                                    left_on=[dict_merge_sequence[key][0][1]],
                                                    right_on=[dict_merge_sequence[key][1][1]])
        else:
            merge_result['merge_output'] = pd.merge(merge_result[dict_merge_sequence[key][0][0]],
                                                    dict_node_frame[dict_merge_sequence[key][1][0]],
                                                    how='inner',
                                                    left_on=[dict_merge_sequence[key][0][1]],
                                                    right_on=[dict_merge_sequence[key][1][1]])
    # Sort the output based on cycle.@id
    merge_output = merge_result['merge_output'][input_file.columns].sort_values(by=['cycle.@id'], ignore_index=True)
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
