import boto3
import requests
from io import BytesIO
from datetime import datetime

# Initialize Boto3 S3 client
s3 = boto3.client('s3')

# Define your S3 bucket name
s3_bucket = 'raw-tripdata-prod-851725399217'

# Define the base URL for the Parquet files
base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'

# Generate a list of URLs for the Parquet files from 2021 to 2024
years = range(2021, 2025)
months = range(1, 13)
yellow_urls = [f'{base_url}yellow_tripdata_{year}-{month:02d}.parquet' for year in years for month in months]
green_urls = [f'{base_url}green_tripdata_{year}-{month:02d}.parquet' for year in years for month in months]

def upload(urls: list, prefix: str):
    # Iterate over each URL, download the Parquet file, and upload it to your S3 bucket
    for url in urls:
        # Download Parquet file from URL
        # breakpoint()
        response = requests.get(url)
        if response.status_code == 200:
            parquet_content = BytesIO(response.content)
            
            # Extract the filename from the URL
            filename = url.split('/')[-1]

            # Upload Parquet file to S3 bucket
            s3_key = f'{prefix}/{filename}'
            s3.upload_fileobj(parquet_content, s3_bucket, s3_key)
            print(f'Uploaded {filename} to S3 bucket {s3_bucket}')
        else:
            print(f'Failed to download {url}')

    print('Done!')


prefixes = ['yellow', 'green']
urls = [yellow_urls, green_urls]
for u in urls:
    upload(u, prefixes[urls.index(u)])
