import boto3
import psycopg2
from botocore.exceptions import ClientError

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
        
def create_redshift_tables_from_parquet(schema_name, redshift_creds, s3_creds, s3_bucket, parquet_files):
    # Initialize Redshift Data API client
    redshift_client = boto3.client('redshift-data', region_name=redshift_creds['region'])

    # Define SQL commands
    sql_commands = [
    f"CREATE SCHEMA IF NOT EXISTS {schema_name};",
    f'''
        CREATE TABLE IF NOT EXISTS {schema_name}.Customer (
            CustomerID INT PRIMARY KEY,
            FirstName VARCHAR(255) NOT NULL,
            LastName VARCHAR(255) NOT NULL,
            Email VARCHAR(255) NOT NULL,
            Phone VARCHAR(20) NOT NULL
        );
    ''',
    f'''
        CREATE TABLE IF NOT EXISTS {schema_name}.Product (
            ProductID INT PRIMARY KEY,
            ProductName VARCHAR(255) NOT NULL,
            ProductDescription VARCHAR(255) NOT NULL,
            ProductPrice DECIMAL(10, 2) NOT NULL,
            ProductInventory INT NOT NULL
        );
    ''',
    f'''
        CREATE TABLE IF NOT EXISTS {schema_name}."Order" (
            OrderID INT PRIMARY KEY,
            CustomerID INT NOT NULL,
            OrderDate DATE NOT NULL,
            TotalAmount DECIMAL(10, 2) NOT NULL
        );
    ''',
    f'''
        CREATE TABLE IF NOT EXISTS {schema_name}.OrderItem (
            OrderItemID INT PRIMARY KEY,
            OrderID INT NOT NULL,
            ProductID INT NOT NULL,
            Quantity INT NOT NULL,
            Price DECIMAL(10, 2) NOT NULL
        );
    ''',
    f"ALTER TABLE {schema_name}.\"Order\" ADD FOREIGN KEY (CustomerID) REFERENCES {schema_name}.Customer(CustomerID);",
    f"ALTER TABLE {schema_name}.OrderItem ADD FOREIGN KEY (OrderID) REFERENCES {schema_name}.\"Order\"(OrderID);",
    f"ALTER TABLE {schema_name}.OrderItem ADD FOREIGN KEY (ProductID) REFERENCES {schema_name}.Product(ProductID);"
]
        

    # Execute SQL commands to create table
    for sql in sql_commands:
        execute_redshift_query(redshift_client, redshift_creds['cluster_identifier'], redshift_creds['dbname'], redshift_creds['user'], redshift_creds['password'], redshift_creds['host'], redshift_creds['port'], sql)

        

# Load Parquet data into Redshift
    for parquet_file in parquet_files:
        table_name = parquet_file.split('/')[-2]
        copy_statement = f'''
        COPY {schema_name}.{table_name} 
        FROM '{parquet_file}'
        IAM_ROLE '{redshift_creds["iam_role"]}'
        FORMAT AS PARQUET;
        '''
        execute_redshift_query(redshift_client, redshift_creds['cluster_identifier'], redshift_creds['dbname'], redshift_creds['user'], redshift_creds['password'], redshift_creds['host'], redshift_creds['port'], copy_statement)

# connection to S3 to get path for parquet files
s3_bucket = 'wu1process'
schema_name = 'public'
prefix = 'delta/'
s3 = boto3.client('s3')
result = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

parquet_files = []
for item in result.get('Contents', []):
    if item['Key'].endswith('.snappy.parquet'):
        file_path = f's3://{s3_bucket}/{item["Key"]}'
        parquet_files.append(file_path)

# Redshift credentials
redshift_creds = {
    'cluster_identifier': 'redshift-cluster-1',
    'region': 'us-east-1',
    'dbname': 'dev',
    'user': 'wu-user-redshift',
    'password': 'Wu1-pass-redshift',
    'host': 'redshift-cluster-1.cem1k8vpfcmb.us-east-1.redshift.amazonaws.com',
    'port': 5439,
    'iam_role': 'arn:aws:iam::222498481656:role/service-role/AmazonRedshift-CommandsAccessRole-20230315T175615'
}

# S3 credentials
s3_creds = {
    'aws_access_key_id': 'AKIATHTPRMX4PSW55A6O',
    'aws_secret_access_key': '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'
}

create_redshift_tables_from_parquet(schema_name, redshift_creds, s3_creds, s3_bucket, parquet_files)
