from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import (col ,when)
import argparse
from datetime import timedelta,date


# Create SparkSession
spark = SparkSession.builder.appName('Non_encrypted_id').getOrCreate()

# Create the parser
my_parser = argparse.ArgumentParser(description='Non_encrypted_id')

# Add the arguments
my_parser.add_argument('inputpath',
                       type=str,
                       help='the input path for source data.')
my_parser.add_argument('outputpath',
                       type=str,
                       help='the output path for source data.')

args = my_parser.parse_args()
inputpath = args.inputpath
outputpath = args.outputpath

# inputpath="s3://data-sales-dashboard/sample/aman/non_encrypted_id_script/"
# outputpath="s3://data-sales-dashboard/output/aman/non_encrypted_id_output/"

# Logic for Last "7 Days"
inputpath_with_date=[]
yesterday = (date.today() - timedelta(days=1))
for dates in range(7):
    date_folder = (yesterday - timedelta(days=dates)).strftime("%Y/%m/%d")
    input_path_date=inputpath+date_folder+"/*"
    inputpath_with_date.append(input_path_date)
inputpath_with_date

today = date.today()
today=today.strftime("%Y%m%d")

file_schema = StructType([
                        StructField('date',StringType(),True),
                        StructField('time',StringType(),True),
                        StructField('uuid',StringType(),True),
                        StructField('xforwardedfor',StringType(),True),
                        StructField('lidid',StringType(),True),
                        StructField('useragent',StringType(),True),
                        StructField('requestenvironment',StringType(),True),
                        StructField('referer',StringType(),True),
                        StructField('requesttype',StringType(),True),
                        StructField('query',StringType(),True),
                        StructField('countrylookup',StringType(),True),
                        StructField('clientname',StringType(),True),
                        StructField('clientversion',StringType(),True),
                        StructField('destination',StringType(),True),
                        StructField('publisherid',StringType(),True),
                        StructField('unifiedid',StringType(),True),
                        StructField('mostlikelyemailhash',StringType(),True),
                        StructField('origin',StringType(),True),
                        StructField('refererapexdomain',StringType(),True),
                        StructField('usedidentifier',StringType(),True),
                        StructField('originapexdomain',StringType(),True),
                        StructField('mappingtypes',StringType(),True),
                        StructField('nonid',StringType(),True),
                        StructField('encryptedid',StringType(),True),
                        StructField('year_p',StringType(),True),
                        StructField('month_p',StringType(),True),
                        StructField('day_p',StringType(),True),
                        ])

resultant_dataframe = spark.createDataFrame(spark.sparkContext.emptyRDD(),file_schema)
r=0
try:
    for m in range(1,8):
        df=spark.read.csv(inputpath_with_date[r], sep='\t',schema=file_schema)
        df=df.withColumn('Result',when(df.date=='date',"True") \
             .otherwise("False")).filter(col("Result")=="False").drop("Result")
        df.persist()
        resultant_dataframe=df.unionByName(resultant_dataframe)
        df.unpersist()
        r+=1
except:
    raise Exception(f"\n\n\n+---------------------------------------------------------------------------------------------------------------+\n|     PATH DOES NOT EXIST FOR == {input_path_date[r]}     |\n+---------------------------------------------------------------------------------------------------------------+\n\n\n")


########################  DATA TRANFORMATION CODE GOES HERE  ########################








############################  WRITING OUTPUT DATA  ##############################

resultant_dataframe.write.csv("{}{}".format(outputpath,today),sep="\t",header=True,mode="overwrite")
