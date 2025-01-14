import os
import urllib.request
import ssl
from operator import concat

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
os.environ['HADOOP_HOME'] = r'C:/Users/User/IdeaProjects/FirstProject/hadoop'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################



print("===========STARTED=============")

data = sc.textFile("dt.txt")
print("=============raw rdd=======")
data.foreach(print)

mapsplit = data.map(lambda x:x.split(","))
print("========mapsplit rdd========")
mapsplit.foreach(print)

from collections import namedtuple

columns = namedtuple('columns',['id','tno','amt','category','product','mode'])
assigncol = mapsplit.map(lambda x: columns(x[0],x[1],x[2],x[3],x[4],x[5]))
prodfilter = assigncol.filter(lambda x: 'Gymnastics' in x.product)
print("======prodfilter rdd=========")
print()
prodfilter.foreach(print)
df = prodfilter.toDF()

df.show()

df.createOrReplaceTempView("trump")

spark.sql("select id,tno from trump").show()


####rdd=sc.textFile("C:\\Users\\User\\IdeaProjects\\FirstProject\\dt.txt")
print("========= DATA PREPARATION======")

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()


# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")


csvdf = (

    spark
    .read
    .format("csv")
    .option("header","true")
    .load("df.csv")

)

csvdf.show()


print("****second way to read cs file****")
scv_df=spark.read.csv("df.csv")
scv_df.show(10)

jsondf = (

    spark
    .read
    .format("json")
    .load("file4.json")
)


jsondf.show(1000)


print("**first way to read parquet file**")
parquetdf = (

    spark
    .read
    .format("parquet")
    .load("file5.parquet")

)
parquetdf.show()

print("**Second way to read parquet file**")
df_parquet=spark.read.parquet("file5.parquet")
df_parquet.show(10)

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()

seldf = df.select("tno","tdate")

seldf.show()

dropdf = df.drop("tno","tdate")

dropdf.show()


data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()


sincol= df.filter("  category='Exercise'  ")

sincol.show()

# Muti Col Filter

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()



mulcol = df.filter("  category='Exercise' and spendby='cash'    ")

mulcol.show()

mulcolor = df.filter("  category='Exercise' or spendby='cash'    ")

mulcolor.show()

# Mul Value Session


data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()


mulval = df.filter("  category   in  ('Exercise','Gymnastics')     ")

mulval.show()

# ALL FILTERS


data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()



sincol= df.filter("  category='Exercise'  ")

sincol.show()





mulcol = df.filter("  category='Exercise' and spendby='cash'    ")

mulcol.show()

mulcolor = df.filter("  category='Exercise' or spendby='cash'    ")

mulcolor.show()





mulval = df.filter("  category   in  ('Exercise','Gymnastics')     ")

mulval.show()

mulval = df.filter("product is null")
mulval.show()

mulval = df.filter("product is not null")
mulval.show()

################# data frame operations ############

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]
df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()
from pyspark.sql.functions import *

prodfilter = df.filter(" product not like '%Gymnastics%' ")
prodfilter.show()


nullfilter = df.filter("product is null")
nullfilter.show()


notnullfilter = df.filter("product is not null")
notnullfilter.show()



notfilter = df.filter("category != 'Exercise'")
notfilter.show()

####MAKE -- CATEGORY HAS UPPER
######MAKE ---PRODUCT AS LOWER
#######MAKE ---cast tno as int
data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()

from pyspark.sql.functions import *  #🔴🔴


procdf = df.selectExpr(

    "cast(tno as int) as tno",  #processing
    "split(tdate,'-')[2] as tdate",   #processing
    "amount+100 as amount", #processing
    "  upper(category) as category ",  # Expression
    "concat(product,'~zeyo') as product", #processing
    "spendby",#Column
    """case 
       when spendby='cash' then 0
       when spendby='paytm' then 1
       else 2
       end as status"""

)
procdf.show()

from pyspark.sql.functions import *  #🔴🔴

withcolexp =(
                df.withColumn("tno",expr("cast(tno as int)"))
                  .withColumn("tdate",expr("split(tdate,'-')[2]"))
                  .withColumn("amount",expr("amount+100"))
                  .withColumn("category",expr("upper(category)"))
                  .withColumn("product",expr("concat(product,'~zeyo')"))
                  .withColumn("status",expr("case when spendby='cash' then 0 when spendby='paytm' then 2 else 1 end"))
                  .withColumnRenamed("tdate","year")
)
withcolexp.show()

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


df = spark.read.format("csv").option("mode","failfast").option("header","true").load("usdata.csv")
df.show()


df.write.format("parquet").mode("overwrite").save(r'C:\Users\User\IdeaProjects\FirstProject\parquetdata')



