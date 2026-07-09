import boto3
from config import AWS_REGION

kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)