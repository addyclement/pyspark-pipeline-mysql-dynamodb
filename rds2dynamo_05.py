import pandas as pd
import boto3
from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from datetime import datetime 

# sc = SparkSession.builder.getOrCreate()

# sc.sparkContext.setLogLevel("ERROR")

# sc.conf.set("spark.sql.execution.arrow.enabled","true")
# sqlContext = sql.SQLContext(sc)

spark = SparkSession \
        .builder \
        .appName("dynamodb") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#create a schema for the data


#create a dataframe for daily nipdata from slave db

# /usr/local/spark/jars mysql drivers here

nipdata_df = spark.read.format("jdbc").options(
url='jdbc:mysql://source-db.ce6wraevepxh.us-east-1.rds.amazonaws.com:3306/dynamodb',
driver='com.mysql.cj.jdbc.Driver',
user='dynamo',
password='password').option("query", "SELECT branch_code, acct_number, acct_type,trx_type,trx_amount,CAST(trx_date AS char) as trx_date, employee_id FROM dynamodb.glen_bank_daily_summary limit 500000").load()

#driver='com.mysql.cj.jdbc.Driver',
#print the schema just for view only purpose, preview the top 3 entries

nipdata_df.printSchema()
# nipdata_df.show(5,truncate= False)

print("started :", datetime.now()) 


pandasDF = nipdata_df.toPandas()



dyn_resource = boto3.resource('dynamodb',region_name='us-east-1')

table = dyn_resource.Table('glen_bank')
pandasDF = nipdata_df.toPandas()
   
starts = 0

for i in range(19998):
     
    
    splits = pandasDF.iloc[starts:starts+25]

    with table.batch_writer() as batch:
        
        for index, row in splits.iterrows():
            #print(index)
                # print(row['acct_number'], row['trx_amount'])

            batch.put_item(
                Item={
                        'acct_number': row['acct_number'],
                        'trx_date': row['trx_date'],
                        'trx_amount': row['trx_amount'],
                        'branch_code': row['branch_code'],
                        'acct_type': row['acct_type'],
                        'trx_type': row['trx_type'],
                        'employee_id': row['employee_id'],
                        'trx_amount': row['trx_amount']
                    }
                )

    print("index start :" + str(starts))
    starts+=25
    print("batch number :"+ str(i))

print("ended at :", datetime.now()) 
    


#https://sparkbyexamples.com/pyspark/pyspark-loop-iterate-through-rows-in-dataframe/

