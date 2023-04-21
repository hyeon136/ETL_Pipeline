from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, col, get_json_object, hour, first, to_timestamp, last
from pyspark.sql.types import StringType, IntegerType
import udf_function as uf
import os

spark = SparkSession.builder.appName("etl pipeline transfomr").master("local[*]").getOrCreate()

df = spark.read.json("./tmp.json")

df = df.drop("recordId")
df = df.drop("ArrivalTimeStamp")

decrypt_udf = udf(uf.decrypt_data, StringType())

df = df.withColumn("data", decrypt_udf(col('data')))

df = df.withColumn("user_id", get_json_object(col('data'), "$.user_id"))\
.withColumn("record_id", get_json_object(col('data'), "$.record_id"))\
.withColumn("activity", get_json_object(col('data'), "$.activity"))\
.withColumn("url", get_json_object(col('data'), "$.url"))\
.withColumn("method", get_json_object(col('data'), "$.method"))\
.withColumn("name", get_json_object(col('data'), "$.name"))\
.withColumn("activity", get_json_object(col('data'), "$.activity"))\
.withColumn("inDate", get_json_object(col('data'), "$.inDate"))\
.withColumn("detail", get_json_object(col('data'), "$.detail"))\
.drop("data")

uuid_udf = udf(uf.convert_uuid, StringType())
method_udf = udf(uf.method_mapping, IntegerType())
url_udf = udf(uf.url_mapping, StringType())
inDate_udf = udf(uf.convert_inDate, StringType())

df = df.withColumn("user_id", uuid_udf(col('user_id')))\
.withColumn("method", method_udf(col('method')))\
.withColumn("url", url_udf(col('url')))\
.withColumn("inDate", inDate_udf(col('inDate')))

df = df.withColumn('dt',to_timestamp(col('inDate'), 'yyMMddHHmmss'))

h = df.first()['dt'].hour

df1 = df.select('*').where(h == hour('dt')).drop('dt')
df2 = df.select('*').where(h != hour('dt')).drop('dt')

if df1.count() >= 1:
    df1_date = '20' + df1.first().inDate
    df1_last_date = df1.select(last('inDate')).take(1)[0]['last(inDate)']

    df1.write.mode('overwrite').parquet(f"./tmp_file/{df1.first().inDate}~{df1_last_date}.parquet")

if df2.count() >= 1 :
    df2_date = '20' + df2.first().inDate
    df2_last_date = df2.select(last('inDate')).take(1)[0]['last(inDate)']

    df2.write.mode('overwrite').parquet(f"./tmp_file/{df2.first().inDate}~{df2_last_date}.parquet")

spark.stop()
