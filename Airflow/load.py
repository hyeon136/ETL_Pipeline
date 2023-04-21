import os
import shutil
from pyspark.sql import SparkSession
from dotenv import load_dotenv
load_dotenv("./env/key.env")

DEFAULT_PATH = "./etl_spark/tmp_file/"
AWS_PATH = f"s3a://{os.getenv('BUCKET_NAME')}/data/"

spark = SparkSession.builder.appName("etl pipeline load")\
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))\
    .master("local[*]")\
    .getOrCreate()

list_dir = os.listdir(DEFAULT_PATH)

for i in list_dir:
    year = "20"+i[:2]
    month = i[2:4]
    day = i[4:6]
    hour = i[6:8]
    try:
        df = spark.read.parquet(DEFAULT_PATH + i)
        df.write.format("parquet").mode("overwrite").save(f"{AWS_PATH}/{year}/{month}/{day}/{hour}/{i}")
    except Exception as e:
        print(e)
    else:
        shutil.rmtree(f"{DEFAULT_PATH}{i}")

spark.stop()
