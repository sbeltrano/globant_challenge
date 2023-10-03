import csv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Insert a new department
def insert_department(entity, session, csvreader):
    for row in csvreader:
        if len(row) >= 2:  # Ensure at least two columns in each row
            department_name = row[1]  # Use the second column as the department name
            department_id = row[0]  # Use the second column as the department name

            # Check if the department already exists in the database
            existing_department = session.query(entity).filter_by(id=department_id).first()

            if existing_department:
                print(f"Department '{department_name}' already exists in the database.")
            else:
                # Create a new Department object and insert it into the database
                new_department = entity(name=department_name, id=department_id)
                session.add(new_department)
                session.commit()
                print(f"Added department: {department_name}")

# Insert a new employee
def insert_employee(entity, session, csvreader):
    for row in csvreader:
        if len(row) >= 2: 
            name = row[1]  
            id = row[0]  
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
                new_employee = entity(name=name, id=id, date=date_obj, department_id = department_id, job_id = job_id)
                session.add(new_employee)
                session.commit()
                print(f"Added employee: {name}")

# Insert a new department
def insert_jobs(entity, session, csvreader):
    for row in csvreader:
        if len(row) >= 2:  # Ensure at least two columns in each row
            job_name = row[1]  # Use the second column as the department name
            job_id = row[0]  # Use the second column as the department name

            # Check if the department already exists in the database
            existing_job = session.query(entity).filter_by(id=job_id).first()

            if existing_job:
                print(f"Job '{job_name}' already exists in the database.")
            else:
                # Create a new Department object and insert it into the database
                new_job = entity(name=job_name, id=job_id)
                session.add(new_job)
                session.commit()
                print(f"Added job: {job_name}")
    


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