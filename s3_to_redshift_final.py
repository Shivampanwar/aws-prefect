import pandas as pd
import numpy as np

from prefect import flow,task
from prefect_aws import AwsCredentials
import boto3
from prefect.tasks import task_input_hash
import os
from datetime import timedelta
import psycopg2


'''
    Credetials for s3 bucket
'''
redshift_host  = os.environ.get("redshift_host")
port     =  5439
dbname   =  'dev'
user     =  os.environ.get("redshift_db_user")
password =  os.environ.get('redshift_db_password')


# data_web_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-06.csv.gz"
data_web_url = "https://raw.githubusercontent.com/Shivampanwar/Customer-Segmentation/master/customers.csv"
bucket_name = "mlflow-artifacts-remote-buckets"
file_name = "customer_data.csv"
table_name = "customers_data"


@task(log_prints=True,tags=["downloading"], cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=1))
def get_from_web(url,local_file_name):
    '''
        Fetches data from web and save it's locally
    '''
    df = pd.read_csv(url)
    print (df.head(2))
    if os.path.exists('data'):
        df.to_csv('data/{}'.format(local_file_name),index=None)
    else:
        os.mkdir('data')
        df.to_csv('data/{}'.format(local_file_name),index=None)
    print (df.shape)
    return "data/{}".format(local_file_name)

@task()
def write_to_s3(path):
    '''path: Path to database file'''
    aws_credentials_block = AwsCredentials.load("aws-secret-key")
    s3_client = aws_credentials_block.get_boto3_session().client("s3")
    with open(path, "rb") as f:
        s3_client.upload_fileobj(f, bucket_name,path)
    print ("Uploading is successull")
    return "s3://{}/{}".format(bucket_name,path),path

@task(log_prints=True)
def get_schema_from_s3_table(bucket,file_location_in_bucket,table_name="cust_data"):
    '''
        Retrieves schema for a given table in bucket
    '''

    # s3 = boto3.client('s3',
    #                     aws_access_key_id=myacc_key,
    #                     aws_secret_access_key=secret_key)
    aws_credentials_block = AwsCredentials.load("aws-secret-key")
    s3 = aws_credentials_block.get_boto3_session().client("s3")
    print ("connection starting ")
    obj = s3.get_object(Bucket=bucket, Key=file_location_in_bucket)
    df = pd.read_csv(obj['Body'])
    print ("file read from s3")
    return pd.io.sql.get_schema(df.reset_index(drop=True), table_name)


@flow(log_prints=True)
def create_redshift_table(file_location_in_bucket,bucker_object_uri,table_name,host=redshift_host,port=port,dbname=dbname,user=user,password=password):
    '''
        File Location in bucket: simple filection in bucket like data/table_1.csv
        bucker_object_uri: URI for file in the bucket
        table_name: Name of table you want to build in redshift
    '''

    print ("fetching schema from s3 ")
    schema = get_schema_from_s3_table(bucket_name,file_location_in_bucket,table_name)## rremove hardcode
    print ("Schema fetching complete")

    conn = psycopg2.connect(f"host={host} port={port} dbname={dbname} user={user} password={password}")## connection to redshift
    print ("connection to Redshift data base is done")

    cur = conn.cursor()
    cur.execute(schema)
    conn.commit()

    copy_sql = """
    COPY {}
    FROM '{}'
    IAM_ROLE 'arn:aws:iam::703296834220:role/service-role/AmazonRedshift-CommandsAccessRole-20230217T103822'
    FORMAT AS CSV
    IGNOREHEADER 1
    """.format(table_name,bucker_object_uri)
    print (copy_sql)
    cur.execute(copy_sql)
    conn.commit()
    
    cur.close()
    conn.close()




@flow()
def s3_to_redshift():
    pth =  get_from_web(data_web_url,file_name)
    s3_uri_pth,non_uri_pth_bucket = write_to_s3(pth)
    create_redshift_table(non_uri_pth_bucket,s3_uri_pth,table_name)
    


if __name__=="__main__":
    s3_to_redshift()


