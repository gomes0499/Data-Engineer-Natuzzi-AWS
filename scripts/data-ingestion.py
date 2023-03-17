import json
import boto3

# Create function called to start replication task
def start_dms_task(region, source_arn, target_arn, instance_arn, task_arn):
    dms_client = boto3.client('dms', region_name=region)

    response = dms_client.start_replication_task(
        ReplicationTaskArn = task_arn,
        StartReplicationTaskType = "reload-target"
    )

    return task_arn

# Call the function:
replication_task_arn = start_dms_task(
    region = "us-east-1",
    source_arn = "arn:aws:dms:us-east-1:222498481656:endpoint:wu1-source-rds",
    target_arn = "arn:aws:dms:us-east-1:222498481656:endpoint:OEYKHXGK5EABY5SMWDUVTMM4NZMSVCIJTZLX4UY",
    instance_arn = "arn:aws:dms:us-east-1:222498481656:rep:WBSDFIVNWNSOSUQFISXH77U2GXFA5EMKCUYWM6Q",
    task_arn = "arn:aws:dms:us-east-1:222498481656:task:APXUCMXNUBNDUIG6FRVHSJ4OIUME5FSJGJBKSPY"
)

print(f"Started DMS task!")



