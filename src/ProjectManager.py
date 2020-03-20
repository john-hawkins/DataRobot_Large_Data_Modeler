from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import chardet
import yaml
import os
import re

# Import the file:  ProcessLargeData.py
import src.ProcessLargeData as pld

PROJECTS_FOLDER = './projects/'

# ###################################################################################
def initiate_project(project_name, project_type, file):
	""" CREATE THE PROJECT DIRECTORY AND STORE THE DATASET AND METADATA """

	project_folder = PROJECTS_FOLDER + clean_string(project_name)
	exists = True
	while (exists):
		my_file = Path(project_folder)
		if my_file.is_file():
			print("Project directory exists, modifying")
			project_folder = project_folder + "X"
		else:
			exists = False

	try:
		os.mkdir(project_folder)
	except OSError:
		return  0, ("Creation of the directory %s failed" % project_folder),project_folder

	data_path = project_folder + "/data"
	try:
		os.mkdir(data_path)
	except OSError:
		return  0, ("Creation of the directory %s failed" % data_path),project_folder
 
	rawdata_file_path = data_path + "/" + file.filename

	if file and allowed_file(file.filename):
		filename = secure_filename(file.filename)
		rawdata_file_path = data_path + "/" + filename
		file.save(rawdata_file_path)

	encoding = pld.get_file_encoding(rawdata_file_path)
	print(encoding)
 
	dict_file = {'project_name':project_name,
		'project_type':project_type, 
		'raw_data_path':rawdata_file_path, 
		'encoding':encoding}
	config = project_folder + '/config.yaml'
	with open(config, 'w') as file:
		documents = yaml.dump(dict_file, file, default_flow_style=False)
	print(config)
	return 1, "Success", project_folder


# ###################################################################################
ALLOWED_EXTENSIONS = set(['csv'])
def allowed_file(filename):
	return '.' in filename and \
		filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ###################################################################################
def clean_string(s):
    s = re.sub(r"[^\w\s]", '', s)
    s = re.sub(r"\s+", '_', s)
    return s

# ###################################################################################
def get_column_names(project_folder):
	config = project_folder + '/config.yaml'
	data_path = ''
	with open(config) as file:
		config = yaml.load(file)
		data_path = config['raw_data_path']
	with open(data_path) as f:
		first_line = f.readline()
		return first_line.split(",")

def get_dataset_stats(project_folder):
        config = project_folder + '/config.yaml'
        data_path = ''
        with open(config) as file:
                config = yaml.load(file)
                data_path = config['raw_data_path']
        return pld.get_file_stats(data_path)


