from pyspark.sql import SparkSession
from utils import load_kafka_stream, write_stream_to_console

spark = SparkSession.builder.appName('Job1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')


df_stedi = load_kafka_stream(
    spark, 
    'stedi-events', 
    'customer string, score float, riskDate date')

df_stedi.createOrReplaceTempView('CustomerRisk')
customerRiskStreamingDF = spark.sql('select customer, score from CustomerRisk')
 
write_stream_to_console(customerRiskStreamingDF)