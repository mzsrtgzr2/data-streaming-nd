from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

from utils import load_kafka_stream, write_stream_to_console

# TO-DO: create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic

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
# TO-DO: create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customer_schema = StructType([
    StructField('customerName', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('birthDay', StringType()),  
])

# TO-DO: create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
stedi_schema = StructType([
    StructField('customer', StringType()),
    StructField('score', FloatType()),
    StructField('riskDate', DateType()),
])

#TO-DO: create a spark application object
spark = SparkSession.builder.appName('Job2').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
#TO-DO: set the spark log level to WARN

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


write_stream_to_console(emailAndBirthYearStreamingDF)