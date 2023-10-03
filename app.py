# insert_csv.py
from flask import Flask, request, jsonify
from flask_uploads import UploadSet, configure_uploads, DATA
from flask_sqlalchemy import SQLAlchemy
from werkzeug.datastructures import FileStorage
import os
import csv
from sqlalchemy import create_engine, Column, Integer, String, func, extract, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.sql.expression import cast

# insert_csv.py
app = Flask(__name__)

# Configure the SQLite database URI. This will create a database file named 'globant_challenge.db'.
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///globant_challenge.db'

# Determine the absolute path to the directory containing app.py file
app_directory = os.path.dirname(os.path.abspath(__file__))
print(app_directory)
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
    job = db.Column(db.String(128), nullable=False)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    datetime = db.Column(db.DateTime, nullable=True)
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=True)

# Create the database and tables
with app.app_context():
    db.create_all()

app = Flask(__name__)

#Execute other functions
exec(open('./instance/insert_csv.py').read())  # Execute the contents of insert_csv.py

@app.route('/')
def hello_world():
    return 'Santiago Beltrán, Globant Challenge'

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    try:
        print("Request received!")

        if 'csv' in request.files:
            csv_file = request.files['csv']

            if csv_file.filename == '':
                return jsonify({'error': 'No file selected.'}), 400

            if not csv_file.filename.endswith('.csv'):
                return jsonify({'error': 'Invalid file format. Please upload a CSV file.'}), 400
            
            if not csv_file.filename.endswith('.csv'):
                return jsonify({'error': 'Invalid file format. Please upload a CSV file.'}), 400
            
            if csv_file.filename not in ["departments.csv","hired_employees.csv","jobs.csv"]:
                return jsonify({'error': 'Invalid filename format. Please upload a file with the name departments, hired_employees or jobs '}), 400

            try:
                # Save the uploaded CSV file
                filename = csv_file.filename  # Use the original filename
                csv_file.save(os.path.join(os.path.join(app_directory, 'uploads'), filename))
                print(f"CSV file saved as {filename}")
                if filename == "departments.csv":
                    insert_file(os.path.join(os.path.join(app_directory, 'uploads'), filename), filename, Department)
                elif filename == "hired_employees.csv":
                    insert_file(os.path.join(os.path.join(app_directory, 'uploads'), filename), filename, Employee)
                elif filename == "jobs.csv":
                    insert_file(os.path.join(os.path.join(app_directory, 'uploads'), filename), filename, Job)
                
                return jsonify({'message': 'CSV file uploaded and data inserted successfully.'}), 200
            except Exception as e:
                print(f"Error: {str(e)}")
                return jsonify({'error': str(e)}), 500
        else:
            return jsonify({'error': 'No CSV file provided in the request.'}), 400
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/departments_hired_more_than_mean', methods=['GET'])
def get_departments_hired_more_than_mean():
    try:
        engine = create_engine('sqlite:///instance/globant_challenge.db')
        Session = sessionmaker(bind=engine)
        session = Session()
        # Calculate the mean number of employees hired in 2021 for all departments
        employees_per_department_2021 = (
            session.query(
                Department.name.label('Department_Name'),
                func.count().label('Num_Employees_Hired')
            )
            .join(Employee, Employee.department_id == Department.id)
            .filter(
                extract('year', Employee.datetime) == 2021
            )
            .group_by(Department.id, Department.name)
            .all()
        )

        # Calculate the mean of Num_Employees_Hired
        total_employees = 0
        num_departments = 0
        for department in employees_per_department_2021:
            total_employees += department.Num_Employees_Hired
            num_departments += 1
        
        mean_employees_hired = total_employees / num_departments

        # Query to get departments that hired more employees than the mean in 2021
        departments_above_mean = (
            session.query(
                Department.id,
                Department.name,
                func.count().label('num_employees_hired')
            )
            .join(Employee, Employee.department_id == Department.id)
            .filter(
                extract('year', Employee.datetime) == 2021
            )
            .group_by(
                Department.id,
                Department.name
            )
            .having(func.count() > mean_employees_hired)
            .order_by(desc('num_employees_hired'))
            .all()
        )
        
        # Create a list of dictionaries for the response
        response_data = []
        for department in departments_above_mean:
            department_id = department[0]
            department_name = department[1]
            num_employees_hired = department[2]
            
            response_row = {
                'id': department_id,
                'department': department_name,
                'hired': num_employees_hired
            }
            response_data.append(response_row)
        
        return jsonify(response_data), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()
    
@app.route('/employee_hires_2021', methods=['GET'])
def get_employee_hires_2021():
    try:
        engine = create_engine('sqlite:///instance/globant_challenge.db')
        Session = sessionmaker(bind=engine)
        session = Session()
        # Query to get the number of employees hired by department and job in 2021 divided by quarter
        query = (
            session.query(
                Department.name.label('department'),
                Job.job.label('job'),
                (
                    'Q' +
                    (
                        (func.extract('month', Employee.datetime) - 1) / 3 + 1
                    ).cast(Integer).cast(String)
                ).label('quarter'),
                func.count().label('num_employees')
            )
            .join(Department, Employee.department_id == Department.id)
            .join(Job, Employee.job_id == Job.id)
            .filter(
                extract('year', Employee.datetime) == 2021
            )
            .group_by(
                Department.name,
                Job.job,
                'quarter'
            )
            .order_by(
                Department.name,
                Job.job,
                'quarter'
            )
        )
        
        # Execute the query and fetch the results
        results = query.all()
        
        # Create a dictionary to organize the data in the desired format
        formatted_data = {}
        for result in results:
            department_name = result[0]
            job_name = result[1]
            quarter = result[2]
            num_employees = result[3]
            
            # Initialize the department entry if it doesn't exist
            if department_name not in formatted_data:
                formatted_data[department_name] = {}
            
            # Initialize the job entry if it doesn't exist
            if job_name not in formatted_data[department_name]:
                formatted_data[department_name][job_name] = {'Q1': 0, 'Q2': 0, 'Q3': 0, 'Q4': 0}
            
            # Update the count for the corresponding quarter
            formatted_data[department_name][job_name][quarter] = num_employees
        
        
        # Create a list of dictionaries for the response
        response_data = []
        for department_name, department_data in formatted_data.items():
            for job_name, job_data in department_data.items():
                response_row = {
                    'Department_n': department_name,
                    'Job_n': job_name,
                    'Q1': job_data['Q1'],
                    'Q2': job_data['Q2'],
                    'Q3': job_data['Q3'],
                    'Q4': job_data['Q4']
                }
                response_data.append(response_row)
        
        newResponseData = []
        for item in response_data:
                if not(item['Q1'] == 0 and item['Q2'] == 0 and item['Q3'] == 0 and item['Q4'] == 0):
                    response_row = {
                        'Department': item['Department_n'],
                        'E': '',  # Empty second column
                        'Job': item['Job_n'],
                        'Q1': item['Q1'],
                        'Q2': item['Q2'],
                        'Q3': item['Q3'],
                        'Q4': item['Q4']
                    }
                    newResponseData.append(response_row)
        
        return jsonify(newResponseData), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)
    