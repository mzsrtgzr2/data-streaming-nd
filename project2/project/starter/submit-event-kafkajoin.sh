#!/bin/bash
# docker exec -it project2_spark_1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/project/starter/sparkpykafkajoin.py | tee ../../spark/logs/kafkajoin.log 
docker exec -it project2_spark_1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/project/starter/join2.py | tee ../../spark/logs/kafkajoin.log 