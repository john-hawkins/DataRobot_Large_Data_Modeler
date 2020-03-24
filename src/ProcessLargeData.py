from pathlib import Path
import pandas as pd
import chardet
import math
import os
import re
import gzip
import shutil

# ###################################################################################
def compress_file(path_to_file):
    output_file = path_to_file + ".gz"
    with open(path_to_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(path_to_file) 

# ###################################################################################
def sample_data(path_to_file):
    """
    Given a path to a dataset, generate a sample below the required threshold of 1 gig
    """
    gigs = Path(path_to_file).stat().st_size / 1000000000
    sample_prop = round_down(1 / gigs,2)
    line_count = count_lines(path_to_file)
    chunks = round(line_count * sample_prop)
    data_iterator = pd.read_csv(path_to_file, chunksize=chunks, low_memory=False)
    chunk_list = []  
    # Each chunk is in dataframe format
    for data_chunk in data_iterator:  
        filtered_chunk = data_chunk.sample(frac=sample_prop)
        chunk_list.append(filtered_chunk)
    filtered_data = pd.concat(chunk_list)
    return filtered_data

# ###################################################################################
def split_data_by_column_values(path_to_file, split_column, output_path):
    """
    Given a path to a dataset, generate a set of datasets by splitting on one key column
    """
    vals = get_unique_values(path_to_file, split_column)
    for val in vals:
        if acceptable_type(val):
            output_file = output_path + "/" + clean_string( str(val) ) + ".csv"
            subset_data_by_column_value(path_to_file, split_column, val, output_file)
        else:
            print("NULL Value: Ignoring")


# ###################################################################################
def acceptable_type(val):
    if (val is None):
        return False
    if type(val) is str:
        if val.strip():
            return True
        else:
            return False
    elif math.isnan(val):
        return False
    else:
        return True

# ###################################################################################
def subset_data_by_column_value(path_to_file, column_name, column_value, output_file):
    """
    Given a path to a dataset, generate a sample based on the values of a specified column
    """
    gigs = Path(path_to_file).stat().st_size / 1000000000
    sample_prop = round_down(1 / gigs,2)
    line_count = count_lines(path_to_file)
    chunks = round(line_count * sample_prop)
    data_iterator = pd.read_csv(path_to_file, chunksize=chunks, low_memory=False)
    chunk_list = []  
    # Each chunk is in dataframe format
    for data_chunk in data_iterator:  
        filtered_chunk = data_chunk.loc[data_chunk[column_name]==column_value]
        chunk_list.append(filtered_chunk)
    filtered_data = pd.concat(chunk_list)
    filtered_data.to_csv(output_file, header=True, index=False)


# ###################################################################################
def get_file_stats(path_to_file):
    """
    Given a path to a dataset, calculate standard stats
    """
    gigs = Path(path_to_file).stat().st_size / 1000000000
    line_count = 0
    for line in open(path_to_file): 
        line_count += 1
        if line_count==1:
            columns = line.split(",")
            col_count = len(columns)
    return gigs, line_count, col_count, columns 


# ###################################################################################
def get_unique_values(path_to_file, column_name):
    df = pd.read_csv(path_to_file, skipinitialspace=True, usecols=[column_name])
    return df[column_name].unique()


# ###################################################################################
def count_lines(path_to_file):
    """
    Return a count of total lines in a file. In a way that filesize is irrelevant
    """
    count = 0
    for line in open(path_to_file): count += 1
    return count


# ###################################################################################
def round_down(n, decimals=0):
    """
    Round down a number to a specifed number of decimal places
    """
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier


# ###################################################################################
def get_file_encoding(path_to_file):
    rawdata = open(path_to_file, "rb").read()
    enc = chardet.detect(rawdata)
    return enc['encoding']

# ###################################################################################
def clean_string(s):
    s = re.sub(r"[^\w\s]", '', s)
    s = re.sub(r"\s+", '_', s)
    return s



