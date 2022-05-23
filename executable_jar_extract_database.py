import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import functools
from pyspark.sql.functions import countDistinct ,col ,when ,split ,lower, lit,desc
import re
import argparse
import os
import subprocess
import awscli 
from awscli.clidriver import create_clidriver
driver = create_clidriver()

spark = SparkSession.builder \
        .appName("spark_subprocess_custom_jar") \
        .getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", True)


driver.main(f's3 cp s3bucket_path database_path --recursive'.split())


spark_submit_str= "spark-submit s3://bucket/file.jar --db=09 --db_path=/home/hadoop/database/ --separator=\t --output_file=/home/hadoop/input_file.csv"
process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
stdout,stderr = process.communicate()

driver.main(f's3 cp path_where_extracted_input_file_save path_for input_file'.split())

file_schema = StructType([
    StructField("c0", StringType(), True),
    StructField("c1", StringType(), True),
    StructField("c2", StringType(), True)
])
input_file=spark.read.csv(INPUT_PATH,schema=file_schema)

