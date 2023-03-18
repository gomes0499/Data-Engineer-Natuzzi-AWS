import psycopg2
import configparser
import os

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/1-Project/scripts/config/config.ini")


# Replace these variables with your Redshift and S3 credentials
redshift_host = config.get("redshift", "host")
redshift_port = config.getint("redshift", "port")
redshift_user = config.get("redshift", "user")
redshift_password = config.get("redshift", "password")
redshift_db = config.get("redshift", "dbname")
s3_path = "s3://wu1curated/parquet/"
aws_access_key = config.get("aws_creds", "aws_access_key_id")
aws_secret_key = config.get("aws_creds", "aws_secret_access_key")
iam_role = config.get("redshift", "iam_role")

def export_to_s3(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db, s3_path, aws_access_key, aws_secret_key, iam_role):
    conn = psycopg2.connect(
        host=redshift_host,
        port=redshift_port,
        user=redshift_user,
        password=redshift_password,
        dbname=redshift_db
    )

    cursor = conn.cursor()

    # Replace the SELECT query with your denormalized table query
    query = """
    UNLOAD ('SELECT * FROM "dev"."public"."denormalized_table";')
    TO '{}'
    IAM_ROLE '{}'
    FORMAT PARQUET;
    """.format(s3_path, iam_role)

    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print("Error executing UNLOAD command:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


export_to_s3(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db, s3_path, aws_access_key, aws_secret_key, iam_role)
