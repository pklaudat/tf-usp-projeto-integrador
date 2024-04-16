
resource "aws_athena_workgroup" "example" {
  name = "wg${var.project_name}${var.environment}"
  tags = {
    Name = "wg${var.project_name}${var.environment}"
  }
}

resource "aws_glue_catalog_database" "glue_database" {
  name = "db${var.project_name}${var.environment}"
  location_uri = "s3://${var.output_bucket}"
}

resource "aws_glue_catalog_table" "example" {
  name     = "table${var.project_name}${var.environment}"
  database_name = aws_glue_catalog_database.glue_database.name
  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }
  storage_descriptor {
    location      = "s3://${var.bucket_path}"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    compressed    = true
    ser_de_info {
      name                  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "empresa"
      type = "bigint"
    }
    columns {
      name = "data_hora_inicio_viagem"
      type = "timestamp"
    }
    columns {
      name = "data_hora_fim_viagem"
      type = "timestamp"
    }
    columns {
      name = "quantidade_passageiros"
      type = "double"
    }
    columns {
      name = "distancia_viagem"
      type = "double"
    }
    columns {
        name = "local_inicio_viagem_id"
        type = "bigint"
    }
    columns {
        name = "local_fim_viagem_id"
        type = "bigint"
    }
    columns {
        name = "codigo_tarifa"
        type = "double"
    }
    columns {
      name = "forma_pagamento"
      type = "bigint"
    }
    columns {
      name = "tarifa"
      type = "double"
    }
    columns {
      name = "extras"
      type = "double"
    }
    columns {
        name = "imposto_mta"
        type = "double"
    }
    columns {
        name = "taxa_aceno"
        type = "double"
    }
    columns {
        name = "gorjeta"
        type = "double"
    }
    columns {
       name  = "pedagios"
       type =  "double"
    }
    columns {
      name = "custo_total_viagem"
      type = "double"
    }
    columns {
      name = "taxa_congestionamento"
      type = "double"
    }
    # columns {
    #   name = "taxa_aeroporto"
    #   type = "double"
    # }
    columns {
      name = "tipo_taxi"
      type = "string"
    }
  }
}