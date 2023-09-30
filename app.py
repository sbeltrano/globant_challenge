from flask import Flask, request, jsonify
from flask_uploads import UploadSet, configure_uploads, DATA
from flask_sqlalchemy import SQLAlchemy
from werkzeug.datastructures import FileStorage
import os
from datetime import datetime

app = Flask(__name__)

# Configure the SQLite database URI. This will create a database file named 'globant_challenge.db'.
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///globant_challenge.db'

# Determine the absolute path to the directory containing your app.py file
app_directory = os.path.dirname(os.path.abspath(__file__))

# Configure the destination folder for uploaded files (uploads folder in the app directory)
app.config['UPLOADED_CSV_DEST'] = os.path.join(app_directory, 'uploads')

print(os.path.join(app_directory, 'uploads'))

# Create the 'uploads' folder if it doesn't exist
if not os.path.exists(app.config['UPLOADED_CSV_DEST']):
    os.makedirs(app.config['UPLOADED_CSV_DEST'])

csv_files = UploadSet('csv', DATA)
configure_uploads(app, csv_files)

db = SQLAlchemy(app)

# Define SQLAlchemy models the tables (departments, jobs, employees)
class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(128), nullable=False)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    date = db.Column(db.DateTime, nullable=True)
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=True)

# Create the database and tables
with app.app_context():
    db.create_all()

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Santiago Beltrán, Globant Challenge'

@app.route('/upload-csv', methods=['POST'])
def upload_csv():

    print("Request received!")

    if 'csv' in request.files:
        csv_file = request.files['csv']

        if csv_file.filename == '':
            return jsonify({'error': 'No file selected.'}), 400

        if not csv_file.filename.endswith('.csv'):
            return jsonify({'error': 'Invalid file format. Please upload a CSV file.'}), 400

        try:
            # Save the uploaded CSV file
            filename = csv_file.filename  # Use the original filename
            csv_file.save(os.path.join(os.path.join(app_directory, 'uploads'), filename))
            print(f"CSV file saved as {filename}")

            return jsonify({'message': 'CSV file uploaded and data inserted successfully.'}), 200
        except Exception as e:
            print(f"Error: {str(e)}")
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'No CSV file provided in the request.'}), 400

if __name__ == '__main__':
    app.run(debug=True)