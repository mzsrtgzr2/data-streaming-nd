from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType)

from utils import load_kafka_stream, write_stream_to_console

redis_server_change_schema = StructType([
    StructField('key', StringType()),
    StructField('existType', StringType()),
    StructField('Ch', BooleanType()),
    StructField('Incr', BooleanType()),
    StructField('zSetEntries', ArrayType(StructType([
        StructField('element', StringType()),
        StructField('Score', FloatType()),
    ]))),
])

customer_schema = StructType([
    StructField('customerName', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('birthDay', StringType()),  
])

stedi_schema = StructType([
    StructField('customer', StringType()),
    StructField('score', FloatType()),
    StructField('riskDate', DateType()),
])

spark = SparkSession.builder.appName("Job3").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

## REDIS STREAM

df_redis_server = load_kafka_stream(
    spark, 
    'redis-server', 
    redis_server_change_schema)

df_redis_server.createOrReplaceTempView('RedisSortedSet')

df_parse_json_exploded_2 = spark.sql("""
select *, 
       from_json(cast(unbase64(zSetEntries[0].element) as string), 'customerName string, email string, phone string, birthDay string') as customer 
from RedisSortedSet
""")


df_parse_customers = df_parse_json_exploded_2\
    .select(col('customer.*'))
df_parse_customers.createOrReplaceTempView('CustomerRecords')



emailAndBirthDayStreamingDF = spark.sql('select * from CustomerRecords where email is not null and birthDay is not null')
emailAndBirthDayStreamingDF.createOrReplaceTempView('CustomerRecordsValid')


emailAndBirthYearStreamingDF = spark.sql("""
    select *, year(to_date(birthDay, 'yyyy-MM-dd')) as birthYear from CustomerRecordsValid
""")



## STEDI STREAM
df_stedi = load_kafka_stream(
    spark, 
    'stedi-events', 
    'customer string, score float, riskDate date')

df_stedi.createOrReplaceTempView('CustomerRisk')
customerRiskStreamingDF = spark.sql('select customer, score from CustomerRisk')




## JOIN
df_joined = emailAndBirthYearStreamingDF.join(
  customerRiskStreamingDF, 
  expr('customer=email')
  ).selectExpr("customer", "score", "email", "birthYear")
 
write_stream_to_console(df_joined)
df_joined\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafka:19092")\
  .option("topic", "risk-topic")\
  .option("checkpointLocation", "/tmp/kafka-checkpoint-100") \
  .option("failOnDataLoss", "false").start().awaitTermination()
