# %%
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

from pyspark.sql.types import StructType, StructField, LongType, TimestampType, DoubleType, StringType

schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

raw_yellow_df = spark.read.schema(schema).parquet(f"{RAW_DATA_SOURCE}/yellow/")


raw_yellow_df.printSchema()


# %%
# transform dynamic data frame into df
# raw_yellow_df = raw_yellow_dyf.toDF()

# %%
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

# Exiba o esquema atualizado
raw_yellow_df.printSchema()

# Exiba os primeiros registros do DataFrame
raw_yellow_df.show()
print(f"number of rows: {raw_yellow_df.count()}")

# %%
# clean up duplicated and not useful fields
cleaned_yellow_df = raw_yellow_df.dropDuplicates()
cleaned_yellow_df = cleaned_yellow_df.drop("quantidade_passageiros", "store_and_fwd_flag")
print(f"current schema for yellow tripdata:")
cleaned_yellow_df.printSchema()

# %%
# null field analysis
columns_with_nulls = []

# check which columns have null fields
for col_name in cleaned_yellow_df.columns:
    null_count = cleaned_yellow_df.where(col(col_name).isNull()).count()
    if null_count > 0:
        columns_with_nulls.append(col_name)

print("Colunas com valores nulos:", columns_with_nulls)

# replace null values in the taxa_cogestionamento field to 0
cleaned_yellow_df = cleaned_yellow_df.fillna({'taxa_congestionamento': 0.0})
# replace null values in taxa_aeroporto by 0
cleaned_yellow_df = cleaned_yellow_df.fillna({'taxa_aeroporto': 0.0})
# drop rows where codigo_tarifa is null
cleaned_yellow_df = cleaned_yellow_df.na.drop(subset=['codigo_tarifa'])


# %%
df = cleaned_yellow_df
# Extrair o tempo total da viagem em minutos
df = df.withColumn("tempo_total_viagem", (col("data_hora_fim_viagem").cast("timestamp").cast("long") - col("data_hora_inicio_viagem").cast("timestamp").cast("long")) / 60)

# Separar apenas o horário da viagem em uma nova coluna
df = df.withColumn("horario_viagem", expr("substring(data_hora_inicio_viagem, 12, 5)"))

# df.select("taxa_congestionamento").head(20)
df.show(5)


# %%
df.select("horario_viagem").head(10)

# %%
from awsglue.dynamicframe import DynamicFrame

# Assuming 'df' is your DataFrame
# Convert DataFrame to DynamicFrame
print(f"total number of rows after etl: {df.count()}")
trusted_yellow_dyf = DynamicFrame.fromDF(df, glueContext, "TrustedYellowTripdata")

raw_to_trusted_dyf = glueContext.getSink(
  path=f"{TRUSTED_DATA_SOURCE}/yellow",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="trustedYellowTripdata",
  format="parquet"
)
raw_to_trusted_dyf.setCatalogInfo(
  catalogDatabase="yellow-trusted-tripdata", catalogTableName="trusted-yellow-tripdata"
)

trusted_dyf = raw_to_trusted_dyf.writeFrame(trusted_yellow_dyf)

# %%
from awsgluedq.transforms import EvaluateDataQuality


# Create data quality ruleset
ruleset = """Rules = [
            ColumnValues "empresa" in [ 1, 2, 3 ],
            ColumnValues "codigo_tarifa" in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 0.0]
]"""

# Evaluate data quality
dqResults = EvaluateDataQuality.apply(
    frame=trusted_yellow_dyf,
    ruleset=ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "yellowTrustedData",
        "enableDataQualityCloudWatchMetrics": False,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": f"{TRUSTED_DATA_SOURCE}/dq/yellow/"
    },
)


# Inspect data quality results
dqResults.toDF().show()




# # Script generated for node Change Schema
# ChangeSchema_node1712428554271 = ApplyMapping.apply(frame=AmazonS3_node1712428533996, mappings=[("vendorid", "long", "vendorid", "bigint"), ("tpep_pickup_datetime", "timestamp", "tpep_pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "timestamp", "tpep_dropoff_datetime", "timestamp"), ("passenger_count", "double", "passenger_count", "double"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "double", "ratecodeid", "double"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("congestion_surcharge", "double", "congestion_surcharge", "double"), ("airport_fee", "null", "airport_fee", "null")], transformation_ctx="ChangeSchema_node1712428554271")

# # Script generated for node Evaluate Data Quality
# EvaluateDataQuality_node1712428582889_ruleset = """
#     # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
#     Rules = [
#                 ColumnValues "ratecodeid" in [1, 2, 3, 4, 5, 6],
#                 ColumnValues "payment_type" in [1, 2, 3, 4, 5, 6]
#     ]
# """

# EvaluateDataQuality_node1712428582889 = EvaluateDataQuality().process_rows(frame=ChangeSchema_node1712428554271, ruleset=EvaluateDataQuality_node1712428582889_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1712428582889", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING","observations.scope":"ALL"})

# # Script generated for node rowLevelOutcomes
# rowLevelOutcomes_node1712428769875 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1712428582889, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1712428769875")
# ruleOutcomes_node1712428900442 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1712428582889, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1712428900442")
