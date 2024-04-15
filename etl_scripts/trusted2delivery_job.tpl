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
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, DoubleType, StringType, IntegerType
from awsgluedq.transforms import EvaluateDataQuality



sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark._jvm.java.lang.System.setProperty("spark.glue.JOB_NAME", "YellowRawToTrusted")
spark._jvm.java.lang.System.setProperty("spark.glue.JOB_RUN_ID", "jr_1234")



# data sources 
DELIVERY_DATA_SOURCE="s3://${delivery_data_source}"
TRUSTED_DATA_SOURCE="s3://${trusted_data_source}"

# retrieve yellow trusted data
trusted_yellow_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"{TRUSTED_DATA_SOURCE}/yellow/"],
        "recurse": True,
    },
    transformation_ctx="TrustedYellowTripdata",
)

# retrieve green trusted data
trusted_green_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"{TRUSTED_DATA_SOURCE}/green/"],
        "recurse": True,
    },
    transformation_ctx="TrustedGreenTripdata",
)

yellow_df = trusted_yellow_dyf.toDF()
green_df = trusted_green_dyf.toDF()

green_df = green_df.drop("tipo_viagem")
green_df = green_df.withColumn("taxa_aeroporto", SqlFuncs.lit(0.0))
print(len(green_df.columns))
print(len(yellow_df.columns))

df = yellow_df.union(green_df)
df.count()

delivery_dyf = DynamicFrame.fromDF(df, glueContext, "delivery_dyf")

delivery_tripdata = glueContext.getSink(
  path=f"{DELIVERY_DATA_SOURCE}/tripdata",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="TripDataDelivery",
  format="parquet"
)
# write trusted data in s3
delivery = delivery_tripdata.writeFrame(delivery_dyf)
