from copy import copy
from pyspark.sql import SparkSession, DataFrame
#import boto3

session = SparkSession.builder.appName("DataFromS3").config('spark.jars.packages', 'com.audienceproject:spark-dynamodb_2.12:1.1.2').getOrCreate()
dataFrameReader = session.read

responses = dataFrameReader.option("header","true").option("inferSchema",value = True).csv("s3://test-aws-bucket-c4/CUSTOMER_SAMPLE_DATA _2.csv")

print("=== print out schema ====")
responses.printSchema()

#print(responses.collect())


responses.createOrReplaceTempView("CUSTOMER_SAMPLE_DATA_TEMP")
print("Temp View Created")
df3 = session.sql("select customer_sample_data_temp.Costumer_id,customer_sample_data_temp.Txn_date,sum(customer_sample_data_temp.Txn_Amount) as Txn_Amt  from CUSTOMER_SAMPLE_DATA_TEMP group by customer_sample_data_temp.Costumer_id,customer_sample_data_temp.Txn_date")

#print(df3.collect())

spark._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId","AKIAWFBXPLXZZXUBDUBH")
spark._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey","y9cWl3IRbCFkCaASfDM5OBWimKaFTPTUqj19j/kE")

#write to csv
#df3.coalesce(1).write.option("header",'true').csv("s3n://test-aws-bucket-c4/CUSTOMER_SAMPLE_DATA_AGG5.csv")

df3.write.format('jdbc').options(
      url='jdbc:redshift://redshift-cluster-1.c5vcwpwiazyb.us-east-1.redshift.amazonaws.com:5439/dev',	  
      driver='com.amazon.redshift.jdbc42.Driver',
      dbtable='public.customer_aggregate_data',
      user='awsuser',
      password='Carrefour123').mode('append').save() 

