import os
import urllib.request
import ssl
from operator import concat

from Tools.demo.sortvisu import distinct

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()
for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\User\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)

from pyspark.sql.functions import *

##########new scenario############
"""
read complex data types:Array (by using explode function
           struct : retrive by using "." operator

"""

data = [
    (1, [
        {"name": "apple", "color": "red", "quantity": 10, "available": True, "details": {"weight": 200, "origin": "USA"}},
        {"name": "banana", "color": "yellow", "quantity": 5, "available": False, "details": {"weight": 120, "origin": "Mexico"}}
    ]),
    (2, [
        {"name": "orange", "color": "orange", "quantity": 8, "available": True, "details": {"weight": 150, "origin": "Spain"}},
        {"name": "grape", "color": "purple", "quantity": 3, "available": True, "details": {"weight": 50, "origin": "Italy"}}
    ]),
]

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

# Define a more complex schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("fruits", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("available", BooleanType(), True),
        StructField("details", StructType([
            StructField("weight", IntegerType(), True),
            StructField("origin", StringType(), True)
        ]))
    ])), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the original DataFrame
df.show(truncate=False)

# Print schema before explode
df.printSchema()

df1=df.withColumn("fruits",explode(col("fruits")))
df1.printSchema()
df1.show(truncate=False)
df2=(df1.withColumn("name",expr("fruits.name"))
     .withColumn("color",expr("fruits.color"))
     .withColumn("quantity",expr("fruits.quantity"))
     .withColumn("available",expr("fruits.available"))
     .withColumn("details",expr("fruits.details"))
     .withColumn("weight",expr("fruits.details.weight"))
     .withColumn("origin",expr("fruits.details.origin"))
     )
df2.printSchema()
df2.show(truncate=False)
df3=df2.drop("fruits","details")
df3.show(truncate=False)

(df3.write.format("parquet").mode("overwrite")
 .partitionBy("name")
 .save("fruits1"))