import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.functions import col, expr
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# data sources 
RAW_DATA_SOURCE="s3://raw-tripdata-prod-851725399217"
TRUSTED_DATA_SOURCE="s3://trusted-tripdata-prod-851725399217"

# create dynamic frame using s3 as data source
raw_yellow_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={
        "paths": [f"{RAW_DATA_SOURCE}/yellow/"], 
        "recurse": True
    }, 
    transformation_ctx="RawYellow"
)

# transform dynamic data frame into df
raw_yellow_df = raw_yellow_dyf.toDF()

# adapt yellow schema
column_mapping = {
    "VendorID": "empresa",
    "tpep_pickup_datetime": "data_hora_inicio_viagem",
    "tpep_dropoff_datetime": "data_hora_fim_viagem",
    "passenger_count": "quantidade_passageiros",
    "trip_distance": "distancia_viagem",
    "PULocationID": "local_inicio_viagem_id",
    "DOLocationID": "local_fim_viagem_id",
    "RateCodeID": "codigo_tarifa",
    "Payment_Type": "forma_pagamento",
    "fare_amount": "tarifa",
    "extra": "extras",
    "MTA_tax": "imposto_mta",
    "Improvement_surcharge": "taxa_aceno",
    "tip_amount": "gorjeta",
    "tolls_amount": "pedagios",
    "total_amount": "custo_total_viagem",
    "congestion_surcharge": "taxa_congestionamento",
    "airport_fee": "taxa_aeroporto"
}

# rename columns according to the mapping above
for old_col, new_col in column_mapping.items():
    raw_yellow_df = raw_yellow_df.withColumnRenamed(old_col, new_col)

raw_yellow_df.show()
print(f"number of rows: {raw_yellow_df.count()}")


# clean up duplicated and not useful fields
cleaned_yellow_df = raw_yellow_df.dropDuplicates()
cleaned_yellow_df = cleaned_yellow_df.drop("quantidade_passageiros", "store_and_fwd_flag")
print(f"current schema for yellow tripdata:")
cleaned_yellow_df.printSchema()

# null field analysis
columns_with_nulls = []

# check which columns have null fields
for col_name in cleaned_yellow_df.columns:
    null_count = cleaned_yellow_df.where(col(col_name).isNull()).count()
    if null_count > 0:
        columns_with_nulls.append(col_name)

print("Columns with null fields:", columns_with_nulls)

# replace null values in the taxa_cogestionamento field to 0
cleaned_yellow_df = cleaned_yellow_df.fillna({'taxa_congestionamento': 0.0})
# replace null values in taxa_aeroporto by 0
cleaned_yellow_df = cleaned_yellow_df.fillna({'taxa_aeroporto': 0.0})
# drop rows where codigo_tarifa is null
cleaned_yellow_df = cleaned_yellow_df.na.drop(subset=['codigo_tarifa'])

df = cleaned_yellow_df
# Extract total time for a trip in minutes
df = df.withColumn("tempo_total_viagem", (col("data_hora_fim_viagem").cast("timestamp").cast("long") - col("data_hora_inicio_viagem").cast("timestamp").cast("long")) / 60)

# Extract the exact start hour for a trip - no date included
df = df.withColumn("horario_viagem", expr("substring(data_hora_inicio_viagem, 12, 5)"))

df.show(5)
