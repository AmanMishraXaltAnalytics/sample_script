from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import split ,lower
from pyspark.sql.functions import concat_ws,array
from pyspark.sql.functions import explode   
import re
import argparse
from pyspark.sql.functions import trim,lower,upper,when
from pyspark.sql.functions import udf,col
from datetime import date
today = date.today()
today=today.strftime("%Y%m%d")
# Create the parser
my_parser = argparse.ArgumentParser(description='dormant_email_reactivation_airflow')

# Add the arguments
my_parser.add_argument('clientname',
                       type=str,
                       help='the clientname for source data.')

my_parser.add_argument('date_of_input_file',
                       type=str,
                       help='the input date for source data.')


args = my_parser.parse_args()

clientname = args.clientname
date_of_input_file = args.date_of_input_file

spark = SparkSession.builder \
    .appName("dormant_email_reactivation_airflow") \
    .getOrCreate()

schema1 = StructType([
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("lastseen", StringType(), True)
])

read=spark.read.csv("s3://liveintent-dormant-email-reactivation/{}/01.input_data/{}/".format(clientname,date_of_input_file),schema=schema1,sep="\t")
read=read.withColumn('Result',when(read.email=='email',"True").when(read.country=='country',"True").when(read.city=='city',"True").when(read.zip=='zip',"True").when(read.phone=='phone',"True").when(read.lastseen=='lastseen',"True").otherwise("False")).filter(F.col("Result")=="False").drop("Result")
rawemail=read.select("email").withColumnRenamed("email","rawemail")
rawemail.write.csv("s3://liveintent-dormant-email-reactivation/{}/02.rawemails/{}".format(clientname,today),mode="overwrite",sep="\t")

email = read.select("email")

email = email.withColumn("mo", trim(F.col("email")))

def sanitized_email(email):
    PLUS_SIGN_REGEX="\+([a-zA-Z0-9]*)@"
    sanitize=email.replace("@@","@")
    sanitize= re.sub(PLUS_SIGN_REGEX,"@",sanitize)
    
    return sanitize.lower()

def preprocess(sanitize):
    temp = sanitize.split("@")
    temp[0] = temp[0].replace(".","")
    email = '@'.join(temp)
    return email
udf_sanitized_email = udf(sanitized_email,StringType()) 
email=email.withColumn("m",udf_sanitized_email(F.col("mo")))
email = email.withColumn("mou",upper(F.col("mo")))
email = email.withColumn("mol",lower(F.col("mo")))
email = email.withColumn("msu",upper(F.col("m")))
email = email.withColumn("sh",(F.col("m")))
email = email.withColumn("sho",(F.col("mo")))
email = email.withColumn("shol",lower(F.col("mo")))
email = email.withColumn("shou",upper(F.col("mo")))
email = email.withColumn("shsu",upper(F.col("m")))
email = email.withColumn("sh2",(F.col("m")))
email = email.withColumn("sh2o",(F.col("mo")))
email = email.withColumn("sh2ol",lower(F.col("mo")))
email = email.withColumn("sh2ou",upper(F.col("mo")))
email = email.withColumn("sh2su",upper(F.col("m")))
email=email.withColumn("rawemail",(F.col("email")))
email = email.withColumn('emaildomain', split(email['m'], '@').getItem(1))
email = email.withColumn('domain',(F.col("emaildomain"))).drop("email","emaildomain")
    
udf_dict = udf(preprocess,StringType())
email=email.withColumn("mold",udf_dict(F.col("m")))
email=email.withColumn("sh2old",udf_dict(F.col("m")))


email = email.withColumn('m', F.md5(F.col('m')))
email = email.withColumn('mo', F.md5(F.col('mo')))
email = email.withColumn('mol', F.md5(F.col('mol')))
email = email.withColumn('mou', F.md5(F.col('mou')))
email = email.withColumn('msu', F.md5(F.col('msu')))
email = email.withColumn('mold', F.md5(F.col('mold')))


email = email.withColumn('sh', F.sha1(F.col('sh')))
email = email.withColumn('sho', F.sha1(F.col('sho')))
email = email.withColumn('shol', F.sha1(F.col('shol')))
email = email.withColumn('shou', F.sha1(F.col('shou')))
email = email.withColumn('shsu', F.sha1(F.col('shsu')))


email = email.withColumn('sh2', F.sha2(F.col('sh2'), 256))
email = email.withColumn('sh2o', F.sha2(F.col('sh2o'), 256))
email = email.withColumn('sh2ol', F.sha2(F.col('sh2ol'), 256))
email = email.withColumn('sh2ou', F.sha2(F.col('sh2ou'), 256))
email = email.withColumn('sh2su', F.sha2(F.col('sh2su'), 256))
rawemail = email.withColumn('sh2old', F.sha2(F.col('sh2old'), 256))

rawemail = rawemail.select("rawemail","domain","mo","m","mol","mou","msu","sho","sh","shol","shou","shsu","sh2o","sh2","sh2ol","sh2ou","sh2su","mold","sh2old")

rawemail.coalesce(1).write.csv("s3://liveintent-dormant-email-reactivation/{}/03.hashedemails/{}".format(clientname,today),mode="overwrite",sep="\t")

file3=rawemail.drop("rawemail")

file3.write.csv("s3://liveintent-dormant-email-reactivation/{}/04.hashes/{}".format(clientname,today),mode="overwrite",sep="\t")


columns=rawemail.columns
columns.remove('rawemail')
df=rawemail.withColumn("hems",concat_ws(",",array(*columns))).drop(*columns)
df=df.withColumn("hems", split(col("hems"), ","))
df = df.withColumn("hems",explode(df.hems)).dropDuplicates()

df.coalesce(1).write.csv("s3://liveintent-dormant-email-reactivation/{}/05.hashtranslation/{}".format(clientname,today),mode="overwrite",sep="\t")