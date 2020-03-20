from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import chardet
import yaml
import os
import hashlib
import numpy as np
import random
import time
import math

#-------------------------------------------------------------------------------
# The Project class for managing access to resources
#-------------------------------------------------------------------------------
class Project:

    PROJECTS_FOLDER = './projects'
    ALLOWED_EXTENSIONS = set(['csv'])

    ######################################################################
    # CONSTRUCTOR
    ######################################################################
    def __init__(self, project_name, file):
        """ CREATE THE PROJECT AND STORE THE DATA """
        self.project_name = project_name
        self.status = "Initialising..."

        # CREATE THE DIRECTORY
        try:
                os.mkdir(project_folder)
        except OSError:
                self.status = "Failed"
                self.status_message = ("Creation of the directory %s failed" % project_folder)
                return  0, ("Creation of the directory %s failed" % project_folder)
        data_path = project_folder + "/data"
        try:
                os.mkdir(data_path)
        except OSError:
                return  0, ("Creation of the directory %s failed" % data_path)

        rawdata_file_path = data_path + "/" + file.filename
        if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                rawdata_file_path = data_path + "/" + filename
                file.save(rawdata_file_path)

        encoding = pld.get_file_encoding(rawdata_file_path)
        print(encoding)

        dict_file = {'project_name':project_name, 'data_path':rawdata_file_path, 'encoding':encoding}
        config = project_folder + '/config.yaml'
        with open(config, 'w') as file:
                documents = yaml.dump(dict_file, file, default_flow_style=False)
        print(config)
        return 1, "Success"


    # ###################################################################################
    def allowed_file(filename):
        return '.' in filename and \
                filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    # ###################################################################################
    def get_column_names(project_folder):
        return ['test', 'test2']

