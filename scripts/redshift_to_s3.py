import psycopg2

def export_to_s3(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db, s3_path, aws_access_key, aws_secret_key):
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
    IAM_ROLE 'arn:aws:iam::222498481656:role/service-role/AmazonRedshift-CommandsAccessRole-20230315T175615'
    FORMAT PARQUET;
    """.format(s3_path)

    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print("Error executing UNLOAD command:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Replace these variables with your Redshift and S3 credentials
redshift_host = "redshift-cluster-1.cem1k8vpfcmb.us-east-1.redshift.amazonaws.com"
redshift_port = "5439"
redshift_user = "wu1userredshift"
redshift_password = "Wu1passredshift"
redshift_db = "dev"
s3_path = "s3://wu1curated/parquet/"
aws_access_key = "AKIATHTPRMX4PSW55A6O"
aws_secret_key = "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"

export_to_s3(redshift_host, redshift_port, redshift_user, redshift_password, redshift_db, s3_path, aws_access_key, aws_secret_key)
