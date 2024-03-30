#!/bin/bash

echo "Executing sync for ${sync_command}"
aws s3 sync s3://nyc-tlc/"trip data"/ s3://${bucket_name}/${sync_command}/ --exclude "*" --include "${sync_command}_tripdata_2021*.parquet" --include "${sync_command}_tripdata_2022*.parquet"
