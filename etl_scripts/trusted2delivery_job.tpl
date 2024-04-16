import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# data sources 
DELIVERY_DATA_SOURCE="s3://${delivery_data_source}"
TRUSTED_DATA_SOURCE="s3://${trusted_data_source}"


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql("(select * from source1) UNION " + unionType + " (select * from source2)")
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1713221520853 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": [f"{TRUSTED_DATA_SOURCE}/green/"]}, transformation_ctx="AmazonS3_node1713221520853")

# Script generated for node Amazon S3
AmazonS3_node1713223104254 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": [f"{TRUSTED_DATA_SOURCE}/yellow/"], "recurse": True}, transformation_ctx="AmazonS3_node1713223104254")

# Script generated for node Schema Matcher for Union
SchemaMatcherforUnion_node1713223184318 = ApplyMapping.apply(frame=AmazonS3_node1713221520853, mappings=[("empresa", "long", "empresa", "long"), ("data_hora_inicio_viagem", "timestamp", "data_hora_inicio_viagem", "timestamp"), ("data_hora_fim_viagem", "timestamp", "data_hora_fim_viagem", "timestamp"), ("codigo_tarifa", "double", "codigo_tarifa", "double"), ("local_inicio_viagem_id", "long", "local_inicio_viagem_id", "long"), ("local_fim_viagem_id", "long", "local_fim_viagem_id", "long"), ("quantidade_passageiros", "double", "quantidade_passageiros", "double"), ("distancia_viagem", "double", "distancia_viagem", "double"), ("tarifa", "double", "tarifa", "double"), ("extras", "double", "extras", "double"), ("imposto_mta", "double", "imposto_mta", "double"), ("gorjeta", "double", "gorjeta", "double"), ("pedagios", "double", "pedagios", "double"), ("taxa_aceno", "double", "taxa_aceno", "double"), ("custo_total_viagem", "double", "custo_total_viagem", "double"), ("forma_pagamento", "double", "forma_pagamento", "long"), ("taxa_congestionamento", "double", "taxa_congestionamento", "double"), ("tipo_taxi", "string", "tipo_taxi", "string")], transformation_ctx="SchemaMatcherforUnion_node1713223184318")

# Script generated for node Schema Matcher for Union
SchemaMatcherforUnion_node1713223184319 = ApplyMapping.apply(frame=AmazonS3_node1713223104254, mappings=[("empresa", "long", "empresa", "long"), ("data_hora_inicio_viagem", "timestamp", "data_hora_inicio_viagem", "timestamp"), ("data_hora_fim_viagem", "timestamp", "data_hora_fim_viagem", "timestamp"), ("quantidade_passageiros", "double", "quantidade_passageiros", "double"), ("distancia_viagem", "double", "distancia_viagem", "double"), ("codigo_tarifa", "double", "codigo_tarifa", "double"), ("local_inicio_viagem_id", "long", "local_inicio_viagem_id", "long"), ("local_fim_viagem_id", "long", "local_fim_viagem_id", "long"), ("forma_pagamento", "long", "forma_pagamento", "long"), ("tarifa", "double", "tarifa", "double"), ("extras", "double", "extras", "double"), ("imposto_mta", "double", "imposto_mta", "double"), ("gorjeta", "double", "gorjeta", "double"), ("pedagios", "double", "pedagios", "double"), ("taxa_aceno", "double", "taxa_aceno", "double"), ("custo_total_viagem", "double", "custo_total_viagem", "double"), ("taxa_congestionamento", "double", "taxa_congestionamento", "double"), ("tipo_taxi", "string", "tipo_taxi", "string")], transformation_ctx="SchemaMatcherforUnion_node1713223184319")

# Script generated for node Union
Union_node1713223167072 = sparkUnion(glueContext, unionType = "ALL", mapping = {"source1": SchemaMatcherforUnion_node1713223184319, "source2": SchemaMatcherforUnion_node1713223184318}, transformation_ctx = "Union_node1713223167072")

# Script generated for node Change Schema
ChangeSchema_node1713225106860 = ApplyMapping.apply(frame=Union_node1713223167072, mappings=[("empresa", "long", "empresa", "long"), ("data_hora_inicio_viagem", "timestamp", "data_hora_inicio_viagem", "timestamp"), ("data_hora_fim_viagem", "timestamp", "data_hora_fim_viagem", "timestamp"), ("quantidade_passageiros", "double", "quantidade_passageiros", "double"), ("distancia_viagem", "double", "distancia_viagem", "double"), ("codigo_tarifa", "double", "codigo_tarifa", "double"), ("local_inicio_viagem_id", "double", "local_inicio_viagem_id", "long"), ("local_fim_viagem_id", "double", "local_fim_viagem_id", "long"), ("forma_pagamento", "double", "forma_pagamento", "long"), ("tarifa", "double", "tarifa", "double"), ("extras", "double", "extras", "double"), ("imposto_mta", "double", "imposto_mta", "double"), ("gorjeta", "double", "gorjeta", "double"), ("pedagios", "double", "pedagios", "double"), ("taxa_aceno", "double", "taxa_aceno", "double"), ("custo_total_viagem", "double", "custo_total_viagem", "double"), ("taxa_congestionamento", "double", "taxa_congestionamento", "double"), ("tipo_taxi", "string", "tipo_taxi", "string")], transformation_ctx="ChangeSchema_node1713225106860")

# Script generated for node Amazon S3
AmazonS3_node1713223345043 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1713225106860, connection_type="s3", format="glueparquet", connection_options={"path": f"{DELIVERY_DATA_SOURCE}/tripdata/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1713223345043")

job.commit()



