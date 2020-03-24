from flask import Flask, flash, request, redirect, render_template, url_for
from werkzeug.utils import secure_filename

# Import the file:  src/ProcessLargeData.py
import src.ProjectManager as pm
import src.ProcessLargeData as pld

app = Flask(__name__)

# ###################################################################################
# Index Page -- For starting a new project
@app.route('/')
def index():
    # RENDER THE HOME PAGE
    return render_template("index.html")


# ###################################################################################
# 
@app.route('/configure', methods = ['POST', 'GET'])
def configure():
    if request.method == 'POST':
        project_name = request.form["project_name"]
        project_type = request.form["project_type"]

        # ########################################################
        # Check if the post request has the file part
        if 'file' not in request.files:
            message = "No dataset file supplied"
            print("Message: ", message)
            return render_template( "error.html", message=message )
        file = request.files['file']
        # If user does not select file, the browser can submit an empty part without filename
        if file.filename == '':
            message = "Empty filename for dataset"
            print("Message: ", message)
            return render_template( "error.html", message=message )

        result, message, project_folder = pm.initiate_project(project_name, project_type, file)
        features = pm.get_column_names(project_folder)
        gigs, line_count, col_count = pm.get_dataset_stats(project_folder)
 
        return render_template("configure.html", 
            project_type=project_type, 
            project_name=project_name, 
            features=features, gigs=gigs, rows=(line_count-1), cols=col_count)


# ###################################################################################
# 
@app.route('/generate', methods = ['POST', 'GET'])
def generate():
    useable_feats = pld.get_column_names(path_to_data)
    return render_template("generate.html", project=proj, features=useable_feats)


# ###################################################################################
# About Page
@app.route('/about')
def about():
        return render_template("about.html")

# ###################################################################################
# Projects Page -
@app.route('/projects')
def projects():
    # RENDER THE HOME PAGE
    projects = []
    return render_template("projects.html", projects=projects)


# ###################################################################################
# With debug=True, Flask server will auto-reload 
# when there are code changes
if __name__ == '__main__':
	app.run(port=5000, debug=True)


