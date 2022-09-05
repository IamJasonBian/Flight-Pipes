import json
import requests
import pandas as pd
from pandas import json_normalize
import boto3
from datetime import date
from io import StringIO

s3 = boto3.resource('s3')
s3Client = boto3.client('s3')
bucket_name = "flight-ingest"
today = date.today()
Output_Path = today.strftime("%Y/%m/%d") + "/flights.csv"

def handler(event, context):
    url = "https://travelpayouts-travelpayouts-flight-data-v1.p.rapidapi.com/v1/prices/monthly"

    querystring = {"destination":"SEA","origin":"DTW","length":"3","currency":"USD"}
    
    headers = {
    	"X-Access-Token": "26a25271cadd4d97f5f7f3ba3a59be69",
    	"X-RapidAPI-Key": "fc5757d6bcmsh1504128c8d18599p101108jsn63bd4fb92c85",
    	"X-RapidAPI-Host": "travelpayouts-travelpayouts-flight-data-v1.p.rapidapi.com"
    }
    
    response = requests.request("GET", url, headers=headers, params=querystring)
    response_json = json.loads(response.text)

    df = pd.DataFrame([])
    
    #Build monthly best by for next 11 months
    for i in response_json['data']:
        row = json_normalize(response_json['data'][i])
        row['year-month'] = i
        df = df.append(row)
    
    print(df)
    
    #S3 write
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index = False)
    s3Client.put_object(Bucket=bucket_name, Body = csv_buffer.getvalue(), Key = Output_Path)
    
    print(today)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Write Successful at ' + Output_Path)
    }

    
    

    
