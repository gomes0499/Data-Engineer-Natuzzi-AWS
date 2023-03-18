import boto3
import configparser
import os

# Get the full path to the config.ini file
current_dir = os.getcwd()
config_file_path = os.path.join(current_dir, "config", "config.ini")

config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/1-Project/scripts/config/config.ini")


# AWS DMS credentials 
region = config.get("DMS", "region")
source_arn = config.get("DMS", "source_arn")
target_arn = config.get("DMS", "target_arn")
instance_arn = config.get("DMS", "instance_arn")
task_arn = config.get("DMS", "task_arn")


# Create function called to start replication task
def start_dms_task(region, source_arn, target_arn, instance_arn, task_arn):
    dms_client = boto3.client('dms', region_name=region)

    response = dms_client.start_replication_task(
        ReplicationTaskArn = task_arn,
        StartReplicationTaskType = "reload-target"
        )

    return task_arn

# Call the function:
replication_task_arn = start_dms_task(region, source_arn, target_arn, instance_arn, task_arn)

print(f"Started DMS task!")



