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
spark._jvm.java.lang.System.setProperty("spark.glue.JOB_NAME", "YellowTrustedToDelivery")
spark._jvm.java.lang.System.setProperty("spark.glue.JOB_RUN_ID", "jr_1234")


TRUSTED_DATA_SOURCE="s3://${trusted_data_source}"
DELIVERY_DATA_SOURCE="s3://${delivery_data_source}"

# pull the data
trusted_yellow_df = spark.read.parquet(f"{TRUSTED_DATA_SOURCE}/yellow/")

yellow_delivery_dyf = DynamicFrame.fromDF(trusted_yellow_df, glueContext, "trusted_yellow_df")

trusted_to_delivery_dyf = glueContext.getSink(
  path=f"{DELIVERY_DATA_SOURCE}/yellow",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="trustedYellowTripdata",
  format="parquet"
)

delivery_dyf = trusted_to_delivery_dyf.writeFrame(yellow_delivery_dyf)