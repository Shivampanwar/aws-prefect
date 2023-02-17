import pandas as pd
import numpy as np

from prefect import flow,task
from prefect_aws import AwsCredentials
import boto3
from prefect.tasks import task_input_hash
import os
from datetime import timedelta


data_web_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-06.csv.gz"
bucket_name = "mlflow-artifacts-remote-buckets"


@task(log_prints=True,tags=["downloading"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_from_web(url):
    df = pd.read_csv(url)
    if os.path.exists('data'):
        df.to_csv('data/taxi_data.csv',index=None)
    else:
        os.mkdir('data')
        df.to_csv('data/taxi_data.csv',index=None)
    print (df.shape)
    return "data/taxi_data.csv"

@task()
def write_to_s3(path):
    '''path: Path to database file'''
    aws_credentials_block = AwsCredentials.load("aws-secret-key")
    s3_client = aws_credentials_block.get_boto3_session().client("s3")
    with open(path, "rb") as f:
        s3_client.upload_fileobj(f, bucket_name,path)

    print ("Uploading is successull")


@flow()
def web_to_s3():
    pth =  get_from_web(data_web_url)
    write_to_s3(pth)


if __name__=="__main__":
    web_to_s3()


