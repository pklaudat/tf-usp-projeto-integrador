# %%
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, DoubleType, StringType
from awsgluedq.transforms import EvaluateDataQuality


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark._jvm.java.lang.System.setProperty("spark.glue.JOB_NAME", "GreenRawToTrusted")
spark._jvm.java.lang.System.setProperty("spark.glue.JOB_RUN_ID", "jr_1234")

# data sources 
RAW_DATA_SOURCE="s3://${raw_data_source}"
TRUSTED_DATA_SOURCE="s3://${trusted_data_source}"


schema = StructType([
    StructField("vendorid", LongType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("trip_type", DoubleType(), True),
    StructField("ratecodeid", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pulocationid", LongType(), True),
    StructField("dolocationid", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True)
])

# pull the data
# raw_green_df = spark.read.schema(schema).parquet(f"{RAW_DATA_SOURCE}/green/")

raw_green_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={
        "paths": [f"{RAW_DATA_SOURCE}/green/"], 
        "recurse": True
    }, 
    transformation_ctx="RawGreenTripData",
    additional_options={
        "schema": schema
    }
)

change_schema = ApplyMapping.apply(
    frame=raw_green_dyf, 
    mappings=[
        ("vendorid", "long", "vendorid", "long"), 
        ("lpep_pickup_datetime", "timestamp", "lpep_pickup_datetime", "timestamp"), 
        ("lpep_dropoff_datetime", "timestamp", "lpep_dropoff_datetime", "timestamp"), 
        ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), 
        ("ratecodeid", "double", "ratecodeid", "double"), 
        ("pulocationid", "long", "pulocationid", "long"), 
        ("dolocationid", "long", "dolocationid", "long"), 
        ("passenger_count", "double", "passenger_count", "double"), 
        ("trip_distance", "double", "trip_distance", "double"), 
        ("fare_amount", "double", "fare_amount", "double"), 
        ("extra", "double", "extra", "double"), 
        ("mta_tax", "double", "mta_tax", "double"), 
        ("tip_amount", "double", "tip_amount", "double"), 
        ("tolls_amount", "double", "tolls_amount", "double"), 
        ("ehail_fee", "null", "ehail_fee", "null"), 
        ("improvement_surcharge", "double", "improvement_surcharge", "double"), 
        ("total_amount", "double", "total_amount", "double"), 
        ("payment_type", "double", "payment_type", "double"), 
        ("trip_type", "double", "trip_type", "double"), 
        ("congestion_surcharge", "double", "congestion_surcharge", "double")
    ], 
    transformation_ctx="ChangeSchema_node1713117036107"
)

raw_green_df = change_schema.toDF()

# change column names
column_mapping = {
    "VendorID": "empresa",
    "lpep_pickup_datetime": "data_hora_inicio_viagem",
    "lpep_dropoff_datetime": "data_hora_fim_viagem",
    "passenger_count": "quantidade_passageiros",
    "trip_distance": "distancia_viagem",
    "trip_type": "tipo_viagem",
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
}




# rename columns according to the mapping above
for old_col, new_col in column_mapping.items():
    raw_green_df = raw_green_df.withColumnRenamed(old_col, new_col)


# drop duplicates and unuseful columns
cleaned_green_df = raw_green_df.dropDuplicates()
cleaned_green_df = cleaned_green_df.drop("store_and_fwd_flag")
print(f"current schema for green tripdata: {cleaned_green_df.printSchema()}")

# null field analysis
columns_with_nulls = []

# check which columns have null fields
for col_name in cleaned_green_df.columns:
    null_count = cleaned_green_df.where(col(col_name).isNull()).count()
    if null_count > 0:
        columns_with_nulls.append(col_name)

cleaned_green_df.head(5)
  

# drop rows where codigo_tarifa is null
cleaned_green_df = cleaned_green_df.na.drop(subset=['codigo_tarifa'])
cleaned_green_df = cleaned_green_df.filter((col('data_hora_inicio_viagem') >= '2020-01-01') & (col('data_hora_inicio_viagem') <= '2022-12-31'))
cleaned_green_df = cleaned_green_df.filter((col('data_hora_fim_viagem') >= '2020-01-01') & (col('data_hora_fim_viagem') <= '2022-12-31'))
cleaned_green_df.count()



df = cleaned_green_df
df = df.withColumn('tipo_taxi', SqlFuncs.lit('green'))
trusted_green_dyf = DynamicFrame.fromDF(df, glueContext, "TrustedgreenTripdata")

# Create data quality ruleset
ruleset = """Rules = [
            ColumnValues "empresa" in [ 1, 2 ],
            ColumnValues "codigo_tarifa" in [1, 2, 3, 4, 5, 6],
            ColumnValues "forma_pagamento" in [1, 2, 3, 4, 5, 6]
]"""


dq_rows = EvaluateDataQuality().process_rows(
        frame=trusted_green_dyf,
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": "dq_rows",
            "enableDataQualityResultsPublishing": True}, 
        additional_options={
            "performanceTuning.caching":"CACHE_NOTHING",
            "observations.scope":"ALL"
        }
    )
# Script generated for node rowLevelOutcomes
rowOutComes = SelectFromCollection.apply(dfc=dq_rows, key="rowLevelOutcomes", transformation_ctx="greenDataDq")
ruleOutcomes = SelectFromCollection.apply(dfc=dq_rows, key="ruleOutcomes", transformation_ctx="greenDataRulesOutcome")

df = rowOutComes.toDF()
df.show()

# Filter to keep only rows where the Outcome is "Passed"
passed_results_df = df.filter(df['DataQualityEvaluationResult'] == 'Passed')
for dq_column in ["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"]:
    passed_results_df = passed_results_df.drop(dq_column)

# Convert DataFrame back to DynamicFrame
passed_results_dyf = DynamicFrame.fromDF(passed_results_df, glueContext, "passed_results_dyf")

passed_results_df.count()


raw_to_trusted_dyf = glueContext.getSink(
  path=f"{TRUSTED_DATA_SOURCE}/green",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="trustedgreenTripdata",
  format="parquet"
)

dq_trusted = glueContext.getSink(
  path=f"{TRUSTED_DATA_SOURCE}/dq/green",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="trustedgreenTripdata",
  format="parquet"
)


# write trusted data in s3
trusted_dyf = raw_to_trusted_dyf.writeFrame(passed_results_dyf)

# write trusted data quality results
trusted_dq = dq_trusted.writeFrame(ruleOutcomes)

job.commit()