import csv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

def validate_and_trim_string(input_string):
    if not isinstance(input_string, str):
        raise ValueError("Input is not a string")
    
    # Remove leading and trailing whitespace
    trimmed_string = input_string.strip()
    
    return trimmed_string

def validate_integer(input_integer):
    try:
        # Attempt to convert the input string to an integer
        int(input_integer)
    except ValueError:
        raise ValueError("Input is not a valid integer")
    
    return input_integer

# Insert a new department
def insert_department(entity, session, csvreader):
    department_data = []
    for row in csvreader:
        if len(row) >= 2:  # Ensure at least two columns in each row
            try:
                department_name = validate_and_trim_string(row[1])  
                department_id = validate_integer(row[0])
            except:
                continue  
            if not department_name:
                continue
            if not department_id:
                continue

            # Check if the department already exists in the database
            existing_department = session.query(entity).filter_by(id=department_id).first()

            if existing_department:
                print(f"Department '{department_name}' already exists in the database.")
            else:
                # Create a new Department object and insert it into the database
                new_department = {'name':department_name, 'id':department_id}
                department_data.append(new_department)

    batch_size = 100
    for i in range(0, len(department_data), batch_size):
        batch = department_data[i:i + batch_size]
        session.bulk_insert_mappings(entity, batch)
        session.commit()

# Insert a new employee
def insert_employee(entity, session, csvreader):
    employee_data = []
    for row in csvreader:
        if len(row) >= 2: 
            try:
                name = validate_and_trim_string(row[1])  
                id = validate_integer(row[0])
            except:
                continue   
            if not name:
                continue
            if not id:
                continue
            date_obj = None
            try:
                date_obj = datetime.strptime(row[2], "%Y-%m-%dT%H:%M:%SZ") 
            except:
                print("no date")
            department_id = row[3]  
            if not department_id:
                department_id = None

            job_id = row[4] 
            if not job_id:
                job_id = None 

            # Check if the department already exists in the database
            existing_employee = session.query(entity).filter_by(id=id).first()

            if existing_employee:
                print(f"Employee with name '{name}' and id {id} already exists in the database.")
            else:
                # Create a new Department object and insert it into the database
                # new_employee = entity(name=name, id=id, date=date_obj, department_id = department_id, job_id = job_id)
                new_employee = {'name':name, 'id':id, 'datetime':date_obj, 'department_id' : department_id, 'job_id' : job_id}
                employee_data.append(new_employee)

    batch_size = 100
    for i in range(0, len(employee_data), batch_size):
        batch = employee_data[i:i + batch_size]
        session.bulk_insert_mappings(entity, batch)
        session.commit()
            

# Insert a new department
def insert_jobs(entity, session, csvreader):
    job_data = []
    for row in csvreader:
        if len(row) >= 2:
            try:
                job_name = validate_and_trim_string(row[1])  
                job_id = validate_integer(row[0])
            except:
                continue   
            if not job_id:
                continue
            if not job_name:
                continue

            # Check if the department already exists in the database
            existing_job = session.query(entity).filter_by(id=job_id).first()

            if existing_job:
                print(f"Job '{job_name}' already exists in the database.")
            else:
                # Create a new Department object and insert it into the database
                new_job = {'id':job_id,'job':job_name}
                job_data.append(new_job)
                
    batch_size = 100
    for i in range(0, len(job_data), batch_size):
        batch = job_data[i:i + batch_size]
        session.bulk_insert_mappings(entity, batch)
        session.commit()
    


def insert_file(location, filename, entity):
    # Create an SQLAlchemy engine and connect to your database
    engine = create_engine('sqlite:///instance/globant_challenge.db')

    # Create a CSV file reader
    with open(location, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        # ["departments.csv","hired_employees.csv","jobs.csv"]
        # Establish an SQLAlchemy session
        Session = sessionmaker(bind=engine)
        session = Session()

        if filename == "departments.csv":
            insert_department(entity, session, csvreader)
        elif filename == "hired_employees.csv":
            insert_employee(entity, session, csvreader)
        elif filename == "jobs.csv":
            insert_jobs(entity, session, csvreader)

        session.close()