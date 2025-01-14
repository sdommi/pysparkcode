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

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\User\.jdks\corretto-1.8.0_432'
os.environ['HADOOP_HOME']= "hadoop"
conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

data = [("1",)]
df1 =  spark.createDataFrame(data, ["col1"])
df1.show()

data1 = [("2",)]

df2 =  spark.createDataFrame(data1, ["col2"])
df2.show()


uniondf = df1.union(df2).withColumnRenamed("col1","value")
uniondf.show()

data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show()

df1 =df.select('col1').union(df.select('col2')).union(df.select('col3')).union(df.select('col4'))
df1.show()

df2 =df1.withColumnRenamed("col1","col")
df2.show()

from pyspark.sql.functions import *  #🔴🔴

data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
df.show()

totaldata =df.withColumn("total",expr("telugu+english+maths+science+social"))
totaldata.show()

#######joins######

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id", "product"])
df2.show()


innerjoin = df1.join(  df2  ,  ["id"]  ,  "inner")
innerjoin.show()

leftjoin = df1.join(df2,["id"], "left").orderBy("id")
leftjoin.show()

rightjoin = df1.join(df2,["id"],"right").orderBy("id")
rightjoin.show()

fulljoin = df1.join(df2,["id"],"full").orderBy("id")
fulljoin.show()

#### ALL JOIN DIFFERENT COL NAME####
# ALL JOIN DIFFERENT COL NAME

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id1", "product"])
df2.show()


innerjoin = df1.join(df2, df1["id"] == df2["id1"] , "inner").orderBy("id").drop("id1")
innerjoin.show()



leftjoin = df1.join(df2,  df1["id"] == df2["id1"] ,"left").orderBy("id").drop("id1")
leftjoin.show()



rightjoin = df1.join(df2, df1["id"] == df2["id1"]  , "right" ).orderBy("id1").drop("id")
rightjoin.show()


from pyspark.sql.functions import *

fulljoin  = (
    df1.join(df2,  df1["id"] == df2["id1"]  , "full")
    .withColumn("id",expr("case when id is null then id1 else id end"))
    .drop("id1")
    .orderBy("id")

)

fulljoin.show()

source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
],1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
],2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

# Show the DataFrames
df1.show()
df2.show()

print("===== FULL JOIN=====")

fulljoin = df1.join( df2, ["id"], "full" )
fulljoin.show()

from pyspark.sql.functions import *

print("=====NAME AND NAME 1 MATCH=====")

fulljoin = df1.join( df2, ["id"], "full" )
fulljoin.show()

procdf = fulljoin.withColumn("status",expr("case when name=name1 then 'match' else 'mismatch' end"))
procdf.show()

print("=====FILTER MISMATCH=====")

fildf = procdf.filter("status='mismatch'")
fildf.show()

print("=====NULL CHECKS=====")

procdf1 = (

    fildf.withColumn("status",expr("""
                                            case
                                            when name1 is null then 'New In Source'
                                            when name is null  then 'New In target'
                                            else
                                            status
                                            end
                
                
                                            """))

)

procdf1.show()


print("=====FINAL PROC=====")


finaldf = procdf1.drop("name","name1").withColumnRenamed("status","comment")

finaldf.show()

data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]

df1 = spark.createDataFrame(data,["food_id","food_item"])
df1.show()

ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]

df2 = spark.createDataFrame(ratings,["food_id","rating"])
df2.show()

from pyspark.sql.functions import *

leftjoin = df1.join(df2, ["food_id"], "left").orderBy("food_id").withColumn("stats(out of 5)",expr("repeat('*',rating)"))
leftjoin.show()

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id", "product"])
df2.show()



listval = df2.select("id").rdd.flatMap(lambda x : x).collect()
print(listval)

from pyspark.sql import functions as F


finaldf = df1.filter(~F.col('id').isin(listval))
finaldf.show()

antijoin = df1.join(df2, ["id"] , "left_anti")
antijoin.show()

# ANTI JOIN AND CROSS JOIN



data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id", "product"])
df2.show()



listval = df2.select("id").rdd.flatMap(lambda x : x).collect()
print(listval)

from pyspark.sql import functions as F


finaldf = df1.filter(~F.col('id').isin(listval))
finaldf.show()


antijoin = df1.join(df2, ["id"] , "left_anti")
antijoin.show()


crossjoin = df1.crossJoin(df2.withColumnRenamed("id","id1"))
crossjoin.show()

#  CHILD,GRAND PARENTS,PARENTS SCENARIO

data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data, ["child", "parent"])
df.show()


df1  = df

df2  = df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")


df1.show()
df2.show()


joindf = df1.join(df2, df1["child"]==df2["parent1"] ,"inner")
joindf.show()



finaldf = (

    joindf
    .drop("parent1")

)
finaldf.show()


dffinal =(
    finaldf.withColumnRenamed("parent","parent1")
    .withColumnRenamed("child","parent")
    .withColumnRenamed("child1","child")
    .withColumnRenamed("parent1","GrandParent")
    .select("child","parent","GrandParent")
)

dffinal.show()

## AGG


data = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40), ("sai", 10)]

df = spark.createDataFrame(data, ["name", "amount"])

df.show()
df.printSchema()

from  pyspark.sql.functions import *

aggdf = (

    df
    .groupBy("name")
    .agg(
        sum("amount").alias("total") ,
        count("name").alias("cnt")
    )

)

aggdf.show()




data1 = [("sai","chennai", 40), ("sai","hydb", 50), ("sai","chennai", 10), ("sai","hydb", 60)]

df1 = spark.createDataFrame(data1, ["name", "location", "amount"])

df1.show()
df1.printSchema()




from pyspark.sql.functions import *

aggdf1 =(

    df1.groupby("name","location")
    .agg(sum("amount").alias("total"))

)

aggdf1.show()

data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]
df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
df1.show()

data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
df2.show()



joindf = df1.join(df2,df1["dept_id"]==df2["dept_id1"] , "left")

joindf.show()


seldf = joindf.select("emp_id","name","dept_name","salary").orderBy("dept_name","salary")
seldf.show()


from pyspark.sql.functions import *

exprdf =(
    seldf.groupby("dept_name")
    .agg(min("salary").alias("salary"))

)

exprdf.show()



finaldf = exprdf.join(df1,["salary"],"inner").drop("dept_id")

finaldf.select("emp_id","name","dept_name","salary").show()

##########new scenario############
data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")

df.show()

df1= df.select("TeamA","TeamB").distinct()
df1.show()
##df1=df.groupBy('Won').agg(count('Won'))

df = spark.read.format("csv").option("mode","failfast").option("header","true").load("usdata.csv")
df.show()


df.write.format("parquet").mode("overwrite").save("parquetdata")

data1 = [("Alice", 25, "New York"),
         ("Bob", 30, "Los Angeles"),
         ("Alice", 35, "Chicago")]
df_1 = spark.createDataFrame(data1, ["name", "age", "city"])
df_1.show()
grouped_df = df_1.groupBy("name").agg(sum("age").alias("total_age"))
grouped_df.show()

