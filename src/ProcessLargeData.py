from pathlib import Path
import pandas as pd
import chardet
import math
import os
import re

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

def get_file_stats(path_to_file):
        """
        Given a path to a dataset, calculate standard stats
        """
        gigs = Path(path_to_file).stat().st_size / 1000000000
        line_count = 0
        for line in open(path_to_file): 
            line_count += 1
            if line_count==1:
                col_count = len(line.split(","))
        return gigs, line_count, col_count

def count_lines(path_to_file):
	"""
	Return a count of total lines in a file. In a way that filesize is irrelevant
	"""
	count = 0
	for line in open(path_to_file): count += 1
	return count


def round_down(n, decimals=0):
	"""
	Round down a number to a specifed number of decimal places
	"""
	multiplier = 10 ** decimals
	return math.floor(n * multiplier) / multiplier


def get_file_encoding(path_to_file):
	rawdata = open(path_to_file, "rb").read()
	enc = chardet.detect(rawdata)
	return enc['encoding']





