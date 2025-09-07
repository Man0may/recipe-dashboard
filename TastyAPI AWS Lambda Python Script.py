import json
import pandas as pd
import requests
import boto3 
s3 = boto3.client('s3')
def lambda_handler(event, context):
    # TODO implement
    url1 = "https://tasty.p.rapidapi.com/recipes/list"
    querystring = {"from":"0", "size":"100"}
    url2 = "https://tasty.p.rapidapi.com/tags/list"
    headers = {
	"x-rapidapi-key": "YOUR_RAPIDAPI_KEY",
	"x-rapidapi-host": "tasty.p.rapidapi.com"
    }
    response1 = requests.get(url1, headers=headers, params=querystring)
    response2 = requests.get(url2, headers=headers)
    bucket = "tastyapidump"
    dumping = json.dumps(response1.json())
    s3.put_object(Body=dumping, Bucket=bucket, Key='tastyapidump.json')
    dumping = json.dumps(response2.json())
    s3.put_object(Body=dumping, Bucket=bucket, Key='tastytags.json')
    return {
        'statusCode': 200,
        'body': json.dumps('This is sends tags for sorting and recipes to S3.')
    }
