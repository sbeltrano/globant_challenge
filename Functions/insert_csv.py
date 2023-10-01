import csv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Create an SQLAlchemy engine and connect to your database
engine = create_engine('sqlite:///globant_challenge.db')

# Define the Department class representing your table
Base = declarative_base()

class Department(Base):
    __tablename__ = 'department'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

def insert_file(location ):
    # Create a CSV file reader
    with open(location, 'r') as csvfile:
        csvreader = csv.reader(csvfile)

        # Establish an SQLAlchemy session
        Session = sessionmaker(bind=engine)
        session = Session()

        for row in csvreader:
            if len(row) >= 2:  # Ensure at least two columns in each row
                department_name = row[1]  # Use the second column as the department name
                department_id = row[0]  # Use the second column as the department name

                # Check if the department already exists in the database
                existing_department = session.query(Department).filter_by(name=department_name).first()

                if existing_department:
                    print(f"Department '{department_name}' already exists in the database.")
                else:
                    # Create a new Department object and insert it into the database
                    new_department = Department(name=department_name, id=department_id)
                    session.add(new_department)
                    session.commit()
                    print(f"Added department: {department_name}")

        session.close()