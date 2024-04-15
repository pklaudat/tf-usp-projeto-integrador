terraform {
  required_providers {
    postgresql = {
      source = "cyrilgdn/postgresql"
      version = ">= 1.10.0"
    }
  }
}

resource "aws_security_group" "postgresql-sg" {
  name        = "postgresql-${data.aws_region.current.name}-${var.environment}-sg"
  description = "Security Group used to control ingress and egress in postgresql rds instances."
  vpc_id      = data.aws_vpc.default.id
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "db-interconnection" {
  type                     = "ingress"
  description              = "Allow network traffic among the database instances (replication)."
  from_port                = "-1"
  to_port                  = "-1"
  protocol                 = "-1"
  source_security_group_id = aws_security_group.postgresql-sg.id
  security_group_id        = aws_security_group.postgresql-sg.id
}

resource "aws_security_group_rule" "local-connection" {
  type              = "ingress"
  description       = "Allow network traffic from my local workstation."
  from_port         = 5432
  to_port           = 5432
  protocol          = "tcp"
  cidr_blocks       = ["${chomp(data.http.myip.response_body)}/32"]
  security_group_id = aws_security_group.postgresql-sg.id
}

resource "aws_db_parameter_group" "postgresql-db-group" {
  name   = "dbgroup-${data.aws_region.current.name}-${var.environment}"
  family = "postgres13"
  parameter {
    name  = "log_connections"
    value = 1
  }
  parameter {
    name         = "rds.logical_replication"
    value        = 1
    apply_method = "pending-reboot"
  }
}


resource "aws_db_instance" "postgresql" {
  identifier               = "postgresql${var.environment}"
  db_name                  = "postgresql${var.environment}"
  allocated_storage        = var.postgresql_storage_in_gb
  storage_type             = "gp2"
  engine                   = "postgres"
  engine_version           = "13"
  instance_class           = var.postgresql_instance_type
  parameter_group_name     = aws_db_parameter_group.postgresql-db-group.name
  vpc_security_group_ids   = [aws_security_group.postgresql-sg.id]
  multi_az                 = false
  username                 = "postgresqluser"
  password                 = var.postgresql_password
  performance_insights_enabled = true
  skip_final_snapshot      = true
  apply_immediately        = true
  publicly_accessible      = true
  delete_automated_backups = true
  backup_retention_period  = 0
  depends_on               = [aws_db_parameter_group.postgresql-db-group]

}

resource "aws_db_instance" "postgresql-replicas" {
  identifier             = "${aws_db_instance.postgresql.db_name}replica${count.index}"
  engine                 = "postgres"
  engine_version         = "13"
  instance_class         = var.postgresql_instance_type
  parameter_group_name   = aws_db_parameter_group.postgresql-db-group.name
  vpc_security_group_ids = [aws_security_group.postgresql-sg.id]
  multi_az               = false
  skip_final_snapshot    = true
  apply_immediately      = true
  depends_on             = [aws_db_parameter_group.postgresql-db-group]
  replicate_source_db    = aws_db_instance.postgresql.identifier
  count                  = var.postgresql_replicas
}

resource "null_resource" "create_tables" {
  # This null_resource serves as a trigger for the local-exec provisioner
  triggers = {
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = <<EOF
      # Replace the following with your SQL commands to create tables
      psql -h ${aws_db_instance.postgresql.endpoint} -U postgresqluser -d postgresql${var.environment} -p ${var.postgresql_password} -c"
        CREATE TABLE employees (
          employee_id SERIAL PRIMARY KEY,
          first_name VARCHAR(50),
          last_name VARCHAR(50),
          email VARCHAR(100),
          department_id INT,
          hire_date DATE,
          salary DECIMAL(10, 2)
        );

        CREATE TABLE departments (
          department_id SERIAL PRIMARY KEY,
          department_name VARCHAR(100),
          manager_id INT,
          location VARCHAR(100)
        );
      "
EOF
  }
}


output "postgresql_connection_string" {
  value = aws_db_instance.postgresql.endpoint
}