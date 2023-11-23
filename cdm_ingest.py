import csv
import sqlite3
from sqlite3 import Error
from datetime import datetime
import uuid


def create_conn(db_file):
    try:
        conn = sqlite3.connect(db_file)
        print(f'Successful connection with SQLite version {sqlite3.version}')
        return conn
    except Error as e:
        print(e)


def close_conn(conn):
    conn.close()
    print('Database connection closed.')


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_project(conn, project):
    """
    Create a new project into the projects table
    """
    sql = ''' INSERT INTO projects (warehouseProjectId, orgUid, currentRegistry, projectId, originProjectId, registryOfOrigin, program,
              projectName, projectLink, projectDeveloper, sector, projectType, projectTags, coveredByNDC, ndcInformation,
              projectStatus, projectStatusDate, unitMetric, methodology, methodology2, validationBody, validationDate,
              timeStaged, description, createdAt, updatedAt)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()


cdm_org_id = str(uuid.uuid4())

def transform_and_create_projects(datafile, create_func):
    """
    Transform the datafile and populate binding parameters for SQL query for projects table
    """
    with open(datafile, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            warehouseProjectId = str(uuid.uuid4())
            orgUid = cdm_org_id
            currentRegistry = "UNFCCC CDM"  # Not available in the provided data
            projectId = row['Unique project identifier (traceable with Google)']
            originProjectId = None  # Not available in the provided data
            registryOfOrigin = "UNFCCC CDM"  # Not available in the provided data
            program = None
            projectName = row['Registration project title']
            projectLink = "https://cdm.unfccc.int/Projects/Validation/DB/" + row['Unique project identifier (traceable with Google)']
            projectDeveloper = row['DOE']
            sector = row['Sectoral scope number(s)']
            projectType = row['Type of CDM project: PA/PoA']
            projectTags = row['Project classification']
            coveredByNDC = None  # Not available in the provided data
            ndcInformation = None  # Not available in the provided data
            projectStatus = row['Simplified project status']
            projectStatusDate = None #convert_date(row['Date of EB decision (see EB59 Annex 12, §25)'])
            unitMetric = "tCO2"
            methodology = row['Methodologies used at registration'] #Warning: Have multiple methodologies
            methodology2 = None  # Not available in the provided data
            validationBody = row['DOE']
            validationDate = convert_date(row['Start of validation'])
            timeStaged = None  # Not available in the provided data
            description = None  # Not available in the provided data
            createdAt = convert_date(row['PA:Initial registration request/PoA:First CPA request'])
            updatedAt = None

            project = (warehouseProjectId, orgUid, currentRegistry, projectId, originProjectId, registryOfOrigin, program,
                       projectName, projectLink, projectDeveloper, sector, projectType, projectTags, coveredByNDC,
                       ndcInformation, projectStatus, projectStatusDate, unitMetric, methodology, methodology2,
                       validationBody, validationDate, timeStaged, description, createdAt, updatedAt)

            create_func(conn, project)


def convert_date(date_str):
    """
    Convert string date in the format 'DD/M/YYYY' to a datetime object
    """
    print("Going to convert date " + date_str)
    
    try:
        if len(date_str) < 2:
            return None
        elif isinstance(date_str, str):
            return datetime.strptime(date_str, '%d/%m/%y').date()
        else:
            return None
    except Exception as e:
        print("Error parsing date")
        print(e)
        return None


# SQL table creation statements
sql_create_projects_table = """
    CREATE TABLE IF NOT EXISTS projects (
        warehouseProjectId VARCHAR(255) NOT NULL PRIMARY KEY,
        orgUid VARCHAR(255),
        currentRegistry VARCHAR(255),
        projectId VARCHAR(255),
        originProjectId VARCHAR(255),
        registryOfOrigin VARCHAR(255),
        program VARCHAR(255),
        projectName TEXT,
        projectLink TEXT,
        projectDeveloper TEXT,
        sector VARCHAR(255),
        projectType VARCHAR(255),
        projectTags TEXT,
        coveredByNDC VARCHAR(255),
        ndcInformation VARCHAR(255),
        projectStatus VARCHAR(255),
        projectStatusDate DATE,
        unitMetric VARCHAR(255),
        methodology TEXT,
        methodology2 VARCHAR(255),
        validationBody VARCHAR(255),
        validationDate DATE,
        timeStaged VARCHAR(255),
        description TEXT,
        createdAt DATE,
        updatedAt DATE
    );
"""


# Specify the SQLite database file path
db_file = 'cdm_database.db'

# Connect to the database file
conn = create_conn(db_file)


# Create tables
if conn is not None:
    create_table(conn, sql_create_projects_table)
    print('Tables created successfully.')
else:
    print("Cannot connect to the database!")
    exit()

# Load the CSV datasets
activities_file = 'CDM Activities.csv'

# Perform data transformation and insertion into the tables
transform_and_create_projects(activities_file, create_project)

# Print records to verify insertion
cur = conn.cursor()
cur.execute("SELECT * FROM projects")
rows = cur.fetchall()
for row in rows:
    print(row)

# Close the database connection
close_conn(conn)
