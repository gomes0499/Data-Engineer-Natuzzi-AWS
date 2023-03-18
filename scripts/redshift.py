import boto3
import psycopg2
from botocore.exceptions import ClientError
import configparser
import os

# Get the full path to the config.ini file
current_dir = os.getcwd()
config_file_path = os.path.join(current_dir, "config", "config.ini")

config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/1-Project/scripts/config/config.ini")

# AWS Redshift credentials 
redshift = {
    "cluster_identifier": config.get("redshift", "cluster_identifier"),
    "region": config.get("redshift", "region"),
    "dbname": config.get("redshift", "dbname"),
    "user": config.get("redshift", "user"),
    "password": config.get("redshift", "password"),
    "host": config.get("redshift", "host"),
    "port": config.getint("redshift", "port"),
    "iam_role": config.get("redshift", "iam_role"),
}

# AWS Credentials from config.ini
aws_creds = {
    'aws_access_key_id': config.get("aws_creds", "aws_access_key_id"),
    'aws_secret_access_key': config.get ("aws_creds", "aws_secret_access_key")
}

# connection to S3
s3_bucket = 'wu1process'
schema_name = 'public'
prefix = 'parquet/'
s3 = boto3.client('s3')
result = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

# Get path for parquet files
parquet_files = []
for item in result.get('Contents', []):
    if item['Key'].endswith('.snappy.parquet'):
        file_path = f's3://{s3_bucket}/{item["Key"]}'
        parquet_files.append(file_path)


def execute_redshift_query(redshift_client, cluster_id, dbname, user, password, host, port, query):
    # Execute the query
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error executing the query: {e}")
        
def create_redshift_tables_from_parquet(schema_name, redshift, aws_creds, s3_bucket, parquet_files):
    # Initialize Redshift Data API client
    redshift_client = boto3.client('redshift-data', region_name=redshift['region'])

    # Define SQL commands
    sql_commands = [
        f"CREATE SCHEMA IF NOT EXISTS {schema_name};",
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.Category (
                CategoryID INT PRIMARY KEY,
                CategoryName VARCHAR(255) NOT NULL
            );
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.City (
                CityID INT PRIMARY KEY,
                CityName VARCHAR(255) NOT NULL,
                State VARCHAR(255) NOT NULL
            );
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.Customer (
                CustomerID INT PRIMARY KEY,
                CityID INT NOT NULL,
                FirstName VARCHAR(255) NOT NULL,
                LastName VARCHAR(255) NOT NULL,
                Email VARCHAR(255) NOT NULL,
                Phone VARCHAR(20) NOT NULL,
                FOREIGN KEY (CityID) REFERENCES {schema_name}.City(CityID)
            );
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.Product (
                ProductID INT PRIMARY KEY,
                CategoryID INT NOT NULL,
                ProductName VARCHAR(255) NOT NULL,
                ProductDescription VARCHAR(255) NOT NULL,
                ProductPrice DECIMAL(10, 2) NOT NULL,
                ProductInventory INT NOT NULL,
                FOREIGN KEY (CategoryID) REFERENCES {schema_name}.Category(CategoryID)
            );
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}."Order" (
                OrderID INT PRIMARY KEY,
                CustomerID INT NOT NULL,
                OrderDate DATE NOT NULL,
                TotalAmount DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (CustomerID) REFERENCES {schema_name}.Customer(CustomerID)
            );
        ''',
        f'''
            CREATE TABLE IF NOT EXISTS {schema_name}.OrderItem (
                OrderItemID INT PRIMARY KEY,
                OrderID INT NOT NULL,
                ProductID INT NOT NULL,
                Quantity INT NOT NULL,
                Price DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (OrderID) REFERENCES {schema_name}."Order"(OrderID),
                FOREIGN KEY (ProductID) REFERENCES {schema_name}.Product(ProductID)
            );
        '''
    ]
        

    # Execute SQL commands to create table
    for sql in sql_commands:
        execute_redshift_query(redshift_client, redshift['cluster_identifier'], redshift['dbname'], redshift['user'], redshift['password'], redshift['host'], redshift['port'], sql)

        

# Load Parquet data into Redshift
    for parquet_file in parquet_files:
        table_name = parquet_file.split('/')[-2]
        copy_statement = f'''
        COPY {schema_name}.{table_name} 
        FROM '{parquet_file}'
        IAM_ROLE '{redshift["iam_role"]}'
        FORMAT AS PARQUET;
        '''
        execute_redshift_query(redshift_client, redshift['cluster_identifier'], redshift['dbname'], redshift['user'], redshift['password'], redshift['host'], redshift['port'], copy_statement)

create_redshift_tables_from_parquet(schema_name, redshift, aws_creds, s3_bucket, parquet_files)
