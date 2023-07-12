# Databricks notebook source
from pyspark.sql.functions import *
import urllib

# COMMAND ----------

# Define file type
file_type = "delta"
# Whether the file has a header
first_row_is_header = "true"
# Delimiter used in the file
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\
    .load("dbfs:/user/hive/warehouse/amazondataproject_access_keys")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').take(1)[
    0]['Secret access key']

# COMMAND ----------

# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "amazondatarida"
# Mount name for the bucket
MOUNT_NAME = "/mnt/amazondatarida"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY,
                                        ENCODED_SECRET_KEY, AWS_S3_BUCKET)

# COMMAND ----------

# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# It will display all the file in my s3 bucket
display(dbutils.fs.ls("/mnt/amazondatarida/amazon/"))

# COMMAND ----------

# File location and type
file_location = "/mnt/amazondatarida/amazon/All Home and Kitchen.csv"
file_type = "csv"
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
display(df)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_new = spark.read.format('csv').option("header",True).load(
    "/mnt/amazondatarida/amazon/All Home and Kitchen.csv"
)

# COMMAND ----------

df = df_new.na.drop(subset=['discount_price','actual_price'])

# COMMAND ----------

#adding a nw column to the table wit unique ID number
df = df.withColumn("ID", monotonically_increasing_id())


# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.sql import functions as F

# Assuming your DataFrame is named 'df' and the rating column is 'ratings'

# Define the conditions and corresponding values for each rating category
conditions = [
    (col('ratings') > 4.5, 'Excellent Product'),
    ((col('ratings') > 3.8) & (col('ratings') <= 4.5), 'Very good product'),
    ((col('ratings') > 3) & (col('ratings') <= 3.8), 'Good product')
]

# Create a new column 'rating_category' based on the conditions
df = df.withColumn('rating_category', 
                   F.when(conditions[0][0], conditions[0][1])
                   .when(conditions[1][0], conditions[1][1])
                   .when(conditions[2][0], conditions[2][1])
                   .otherwise('Average Product'))

# Fill null values in the 'rating_category' column with a default value (if desired)
df = df.fillna({'rating_category': 'Other'})

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Assuming your DataFrame is named 'df' and the columns are 'actual_price' and 'discount_price'

# Remove dollar ($) and comma (,) symbols from 'actual_price' column
df = df.withColumn('actual_price', regexp_replace('actual_price', '[$,₹]', ''))
df = df.withColumn('discount_price', regexp_replace('discount_price', '[$,₹]', ''))


# Calculate the discount percentage and create a new column 'discount_percentage'
df = df.withColumn('discount_percentage(%)', ((col('actual_price').cast('float') - col('discount_price').cast('float')) / col('actual_price').cast('float')) * 100)


df = df.withColumn('discount_percentage(%)', col('discount_percentage(%)').cast('integer'))

# COMMAND ----------

display(df)

# COMMAND ----------

# write the updated dataset to a new file
df.write.format("csv").option("header", True).save(
    "/mnt/amazondatarida/amazon/Final_All Home and Kitchen.csv")

# COMMAND ----------

# Set up the Snowflake credentials
options = {
    "sfURL": "",
    "sfUser": "",
    "sfPassword": "",
    "sfDatabase": "amazondatabase",
    "sfSchema": "SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
}

# COMMAND ----------

# Write the DataFrame to Snowflake


df = spark.read.format("csv").option("header", "true").load(
    "/mnt/amazondatarida/amazon/Final_All Home and Kitchen.csv").write.format("snowflake").options(**options).mode("append").option("dbtable", "transformed_table").save()

# COMMAND ----------

# query the dataframe in snowflake
df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query", "select * from transformed_table;") \
    .load()


display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# File path of the CSV file in Databricks
file_path = "/mnt/amazondatarida/amazon/Final_All Home and Kitchen.csv"

# Local destination path where the file will be downloaded
local_path = "C:/Users/Rida/Videos/your_file.csv"

# Download the file from Databricks to local
dbutils.fs.cp(file_path, f"file:///{local_path}",recurse=True)

# COMMAND ----------

import pandas as pd

# Convert DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Local destination path where the CSV file will be saved
local_path = "C:/Users/Rida/your_file.csv"

# Save the Pandas DataFrame as a CSV file
pandas_df.to_csv(local_path, index=False)

# COMMAND ----------

# File path of the CSV file in Databricks
file_path = "/mnt/amazondatarida/amazon/Final_All Home and Kitchen.csv"

# Local destination path where the file will be downloaded
local_path = r"C:\Users\Rida\Videos\myfile.csv"

# Download the file from Databricks to local
dbutils.fs.cp(file_path, f"file:///{local_path}",recurse=True)

# COMMAND ----------


