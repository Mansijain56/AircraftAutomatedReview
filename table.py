import sqlalchemy
import pyodbc
from sqlalchemy import text
server = 'mansi-sever.database.windows.net,1433'
database = 'mansi-database'
username = 'mansi_admin_login'
password = '*******'
driver = '{ODBC Driver 17 for SQL Server}' 
print (pyodbc.drivers())

# Create the pyodbc connection string
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
# Connect to the database using pyodbc
connection = pyodbc.connect(connection_string)

# Create the SQLAlchemy engine
engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')


# SQL query to create the 'airline' table
create_table_query = """
CREATE TABLE final_mansi_pro_airline  (
 id INT PRIMARY KEY IDENTITY(1,1),
 recorded_date DATE NULL,
 review_date TEXT NULL,
 airline VARCHAR(255) NULL,
 title TEXT NULL,
 review TEXT NULL,
 over_all_rating TEXT NULL,
 name VARCHAR(255) NULL,
 review_country VARCHAR(255),
 review_verified TEXT NULL,
 type_of_traveller VARCHAR(255) NULL,
 seat_type VARCHAR(255) NULL,
 route VARCHAR(255) NULL,
 date_flown VARCHAR(255) NULL,
 seat_comfort VARCHAR(255) NULL,
 cabin_staff_service VARCHAR(255) NULL,
 food_and_beverages VARCHAR(255) NULL,
 ground_service VARCHAR(255) NULL,
 value_for_money VARCHAR(255) NULL,
 recommended VARCHAR(255) NULL,
 aircraft VARCHAR(255) NULL,
 inflight_entertainment VARCHAR(255) NULL,
 wifi_and_connectivity VARCHAR(255) NULL,
 airline_id VARCHAR(255) NULL,

);

"""
# Execute the SQL query to create the 'airline' table
with connection.cursor() as cursor:
 cursor.execute(create_table_query)
 connection.commit()