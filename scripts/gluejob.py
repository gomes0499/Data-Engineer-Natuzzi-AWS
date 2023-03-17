from pyspark.sql import SparkSession
import boto3 
import os
import configparser

config = configparser.ConfigParser()
config.read("config/config.ini")

# Set credentials for AWS Glue
aws_access_key_id = config.get("aws_creds", "aws_access_key_id")
aws_secret_access_key = config.get("aws_creds", "aws_secret_access_key")
bucket_name = "wu1-glue-job"
job_name = "csv-to-parquet-converter"
script_path = f"s3://{bucket_name}/glue_scripts/data-process.py"


def create_glue_job(job_name, script_path, bucket_name, aws_access_key_id, aws_secret_access_key):
    glue = boto3.client("glue", region_name="us-east-1",
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

    job = glue.create_job(
        Name=job_name,
        Description="Transform CSV data to Parquet format",
        Role="AWSGlueServiceRoleDefault",  # Make sure this role exists and has necessary permissions
        ExecutionProperty={"MaxConcurrentRuns": 1},
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_path,
            "PythonVersion": "3"
        },
        DefaultArguments={
            "--job-language": "python",
            # "--extra-py-files": f"s3://{bucket_name}/delta/delta-core_2.12-1.0.0.jar",
            # "--extra-jars": f"s3://{bucket_name}/delta/delta-core_2.12-1.0.0.jar"
        },
        GlueVersion="3.0",
        WorkerType="Standard",
        NumberOfWorkers=2
    )

    return job["Name"]


create_glue_job(job_name, script_path, bucket_name, aws_access_key_id, aws_secret_access_key)

def start_glue_job(job_name, aws_access_key_id, aws_secret_access_key):
    glue = boto3.client("glue", region_name="us-east-1",
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

    job_run = glue.start_job_run(JobName=job_name)

    return job_run["JobRunId"]

job_run_id = start_glue_job(job_name, aws_access_key_id, aws_secret_access_key)
print(f"Started Glue job {job_name} with JobRunId: {job_run_id}")
