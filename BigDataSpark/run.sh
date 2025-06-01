#!/bin/bash

docker-compose up --build

docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/jars/postgresql.jar /opt/spark-app/spark.py

docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --jars "/opt/jars/postgresql.jar,/opt/jars/clickhouse.jar" --driver-class-path "/opt/jars/postgresql.jar:/opt/jars/clickhouse.jar" /opt/spark-app/click.py