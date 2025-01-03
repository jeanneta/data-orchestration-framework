# Import necessary libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Create Spark Session
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .master("local") \
    .appName("PySpark_Postgres").getOrCreate()

# PostgreSQL credentials
postgres_dwh = {
    'host': '',
    'port': '',
    'db': '',
    'user': '',
    'password': ''
}

postgres_dm = {
    'host': '',
    'port': '',
    'db': '',
    'user': '',
    'password': ''
}

def read_from_postgres(db_config, table_name):
    """Read a table from PostgreSQL into a Spark DataFrame."""
    return spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['db']}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .load()

def save_as_parquet(df, file_name):
    """Save a Spark DataFrame as a Parquet file."""
    df.write.mode('overwrite') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save(file_name)

def export_to_google_sheets(df, sheet_name, worksheet_name, json_key_path):
    """Export a DataFrame to a Google Sheets worksheet."""
    if not isinstance(df, pd.DataFrame):
        df = df.toPandas()

    # Load the Google Sheets API credentials from the JSON key file
    with open(json_key_path, 'r') as file:
        key = json.load(file)

    # Set the scope for Google Sheets API
    scope = ['https://www.googleapis.com/auth/drive', 'https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key, scope)

    # Authorize the client to access Google Sheets API
    client = gspread.authorize(creds)

    # Open the Google Sheet and select the specified worksheet
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)

    # Prepare the data to export: first row is column headers, followed by data rows
    data = [df.columns.values.tolist()] + df.astype(str).values.tolist()

    # Update the worksheet with the data
    worksheet.update(data)

    print(f"Data successfully exported to {sheet_name} - {worksheet_name}")



def extract_load():
    """Perform the ETL process and save intermediate data as Parquet files."""
    # Extract data from PostgreSQL
    fact_employee = read_from_postgres(postgres_dwh, "fact_employee")
    dim_payroll = read_from_postgres(postgres_dwh, "dim_payroll")
    dim_performance = read_from_postgres(postgres_dwh, "dim_performance")
    dim_training = read_from_postgres(postgres_dwh, "dim_training")
    dim_candidate = read_from_postgres(postgres_dwh, "dim_candidate")

    # Create Temp Views
    fact_employee.createOrReplaceTempView("fact_employee")
    dim_payroll.createOrReplaceTempView("dim_payroll")
    dim_performance.createOrReplaceTempView("dim_performance")
    dim_training.createOrReplaceTempView("dim_training")
    dim_candidate.createOrReplaceTempView("dim_candidate")

    # Transformations and Save Results
    # Employee Demographics
    df_employee_demographics = spark.sql('''
        SELECT
            Gender,
            CASE
                WHEN Age BETWEEN 20 AND 29 THEN '20-29'
                WHEN Age BETWEEN 30 AND 39 THEN '30-39'
                WHEN Age BETWEEN 40 AND 49 THEN '40-49'
                ELSE '50+'
            END AS AgeRange,
            COUNT(*) AS TotalEmployees
        FROM fact_employee
        GROUP BY Gender, AgeRange
    ''')
    save_as_parquet(df_employee_demographics, "df_dash_employee_demographics")

    # Candidate Demographics
    df_candidate_demographics = spark.sql('''
        SELECT
            Gender,
            CASE
                WHEN Age BETWEEN 20 AND 29 THEN '20-29'
                WHEN Age BETWEEN 30 AND 39 THEN '30-39'
                WHEN Age BETWEEN 40 AND 49 THEN '40-49'
                ELSE '50+'
            END AS AgeRange,
            COUNT(*) AS TotalCandidates,
            SUM(CASE WHEN OfferStatus = 'Hired' THEN 1 ELSE 0 END) AS PotentialCandidates,
            SUM(CASE WHEN Prediction = 'SAMPLE' THEN 1 ELSE 0 END) AS PredictedPotentialCandidates
        FROM dim_candidate
        GROUP BY Gender, AgeRange
    ''')
    save_as_parquet(df_candidate_demographics, "df_dash_candidate_demographics")

    # HR Costs
    df_hr_costs = spark.sql('''
        SELECT
            DATE_FORMAT(PaymentDate, 'yyyy-MM') AS MonthYear,
            SUM(Salary) AS TotalSalary,
            SUM(OvertimePay) AS TotalOvertimePay,
            SUM(Salary + OvertimePay) AS TotalCost
        FROM dim_payroll dp
        JOIN fact_employee fe ON dp.EmployeeID = fe.EmployeeID
        GROUP BY MonthYear
    ''')
    save_as_parquet(df_hr_costs, "df_dash_hr_costs")

    # Employee Performance
    df_employee_performance = spark.sql('''
        SELECT
            ReviewPeriod,
            AVG(Rating) AS AverageRating,
            SUM(CASE WHEN Rating < 3 THEN 1 ELSE 0 END) AS LowPerformingEmployees
        FROM dim_performance
        GROUP BY ReviewPeriod
    ''')
    save_as_parquet(df_employee_performance, "df_dash_employee_performance")



def load_etl(json_key_path, sheet_name='final_dashboard'):
    """Load Parquet files into PostgreSQL and export to Google Sheets."""
    # PostgreSQL connection details
    host = postgres_dm['host']
    port = postgres_dm['port']
    db = postgres_dm['db']
    user = postgres_dm['user']
    password = urllib.parse.quote_plus(postgres_dm['password'])  # URL encode password

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}', echo=False)

    # Define file-to-table mapping
    file_table_mapping = {
        "df_dash_employee_demographics": "dash_employee_demographics",
        "df_dash_candidate_demographics": "dash_candidate_demographics",
        "df_dash_hr_costs": "dash_hr_costs",
        "df_dash_employee_performance": "dash_employee_performance"
    }

    # Load each Parquet file into PostgreSQL and Google Sheets
    for file_name, table_name in file_table_mapping.items():
        # Read Parquet file
        df = pd.read_parquet(file_name)
        # Write to PostgreSQL
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        # Export to Google Sheets
        export_to_google_sheets(df, sheet_name, table_name, json_key_path)
