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

from  pyspark.sql.window import Window
lisin = [ 1 , 4 , 6 , 7]

rddin = sc.parallelize(lisin)

print("===== RAW RDD =====")

print(rddin.collect())


print()
print("===== ADD RDD =====")

addin  = rddin.map(lambda x : x + 2 )
print(addin.collect())



ls = ["zeyobron" , "zeyo" , "analytics"]

rddstr = sc.parallelize(ls)
print()
print("===== RAW  string RDD =====")
print(rddstr.collect())




filstr = rddstr.filter(lambda x : 'zeyo' in x)
print()
print("===== Filter  string RDD =====")
print(filstr.collect())






file1 = sc.textFile("file1.txt")

gymdata = file1.filter(lambda x : 'Gymnastics' in x)
print()






mapsplit = gymdata.map(lambda x : x.split(","))

from collections import namedtuple

schema = namedtuple('schema',['txnno','txndate','custno','amount','category','product','city','state','spendby'])

schemardd = mapsplit.map(lambda x : schema(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)

schemadf = prodfilter.toDF()

print()
print("===== schema df=====")
print()

schemadf.show(5)




csvdf = spark.read.format("csv").option("header","true").load("file3.txt")
print()
print("===== csvdf df=====")
print()
csvdf.show(5)




jsondf = spark.read.format("json").load("file4.json")
print()
print("===== jsondf df=====")
print()
jsondf.show(5)





parquetdf = spark.read.load("file5.parquet")
print()
print("===== parquetdf df=====")
print()
parquetdf.show(5)



collist = ['txnno','txndate','custno','amount','category','product','city','state','spendby']


jsondf1 = jsondf.select(*collist)
print()
print("===== jsondf1 new df=====")
print()
jsondf1.show(5)




uniondf = schemadf.union(csvdf).union(jsondf1).union(parquetdf)
print()
print("===== uniondf df=====")
print()
uniondf.show(5)


from pyspark.sql.functions import *

procdf =(

    uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
    .withColumnRenamed("txndate","year")
    .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
    .filter("txnno > 50000")
)

print()
print("===== procdf df=====")
print()
procdf.show(5)



aggdf = procdf.groupby("category").agg(sum("amount").alias("total")).withColumn("total",expr("cast(total as decimal(18,2))"))

aggdf.show()




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




leftjoin  = df1.join( df2 , ["id"] , "left").orderBy("id")
leftjoin.show()



rightjoin = df1.join( df2 ,["id"] , "right").orderBy("id")
rightjoin.show()



fulljoin = df1.join(df2, ["id"], "full").orderBy("id")
fulljoin.show()



crossjoin=df1.crossJoin(df2)
crossjoin.show()



leftanti = df1.join(df2,["id"],"left_anti")
leftanti.show()



from pyspark.sql.functions import  *


data = [("DEPT3", 500),
        ("DEPT3", 200),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# 🔴🔴 STEP 1  CREATE WINDOW ON DEPT WITH DESC ORDER OF SALARY

from  pyspark.sql.window import Window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

# 🔴🔴 STEP 2  Applying window on DF with dense rank #########

dfrank = df.withColumn("drank",dense_rank().over(deptwindow))
dfrank.show()

# 🔴🔴 Step 3 Filter Rank 2 and Drop drank #####

finaldf = dfrank.filter("drank=2").drop("drank")
finaldf.show()

## Complex Struct

data="""


{
    "id": 2,
    "trainer": "sai",
    "zeyoaddress": {
            "permanentAddress": "hyderabad",
            "temporaryAddress": "chennai"
    }
}


"""

rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority




flattendf = df.select(
    "id",
    "trainer",
    "zeyoaddress.permanentAddress",
    "zeyoaddress.temporaryAddress"
)

flattendf.show()
flattendf.printSchema()

# Struct inside struct

data="""


{
    "id": 2,
    "trainer": "sai",
    "zeyoaddress": {
        "user": {
            "permanentAddress": "hyderabad",
            "temporaryAddress": "chennai"
      }
    }
}

"""
rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority

flattendf = df.select(
    "id",
    "trainer",
    "zeyoaddress.user.permanentAddress",
    "zeyoaddress.user.temporaryAddress"
)
flattendf.show()
flattendf.printSchema()

# STRUCT DATA FILE

data="""


{
	"place": "Hyderabad",
	"user": {
		"name": "zeyo",
		"address": {
			"number": "40",
			"street": "ashok nagar",
			"pin": "400209"
		}
	}
}


"""

rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority



flattendf = df.select(

    "place",
    "user.address.number",
    "user.address.pin",
    "user.address.street",
    "user.name"

)

flattendf.show()
flattendf.printSchema()

## WITHCOLUMN

data="""



{
    "place": "Hyderabad",
    "user": {
        "name": "zeyo",
        "address": {
            "number": "40",
            "street": "ashok nagar",
            "pin": "400209"
        }
    }
}



"""

rdd = sc.parallelize([data])       # RDD CONVERSION


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()          # second priority
df.printSchema()   # First Priority


from pyspark.sql.functions import *


flattendf = (

    df.withColumn("number", expr("user.address.number"))
    .withColumn("pin", expr("user.address.pin"))
    .withColumn("street", expr("user.address.street"))
    .withColumn("name", expr("user.name"))
    .drop("user")


)

flattendf.show()
flattendf.printSchema()

from pyspark.sql.functions import lead,lag
from pyspark.sql.window import Window
data = [(1111, "2021-01-15", 10),
        (1111, "2021-01-16", 15),
        (1111, "2021-01-17", 30),
        (1112, "2021-01-15", 10),
        (1112, "2021-01-15", 20),
        (1112, "2021-01-15", 30)]

myschema = ["sensorid", "timestamp", "values"]

df = spark.createDataFrame(data, schema=myschema)
df.show()

window_spec = Window.partitionBy("sensorid").orderBy("values")

df = df.withColumn("next_amt", lead("values").over(window_spec))
df = df.withColumn("prev_amt", lag("values").over(window_spec))
df.show()
df=df.withColumn("diff_values",col("next_amt")-col("values"))
df.filter("next_amt is not null").select('sensorid','timestamp','diff_values').show()

#ARRAY#

data="""

{
    "id": 2,
    "trainer": "sai",
    "zeyostudents" : [
    			 "Aarti",
    			 "Arun"    
    ]
}
"""
rdd = sc.parallelize([data])


df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS

df.show()
df.printSchema()

flattendf = df.selectExpr(
    
                        "id",
                        "trainer",
                        "explode(zeyostudents) as students"

)

flattendf.show()
flattendf.printSchema()

####second way to flatten using withCOlumn

flattendf1 = df.withColumn(
    "zeyostudents",expr("explode(zeyostudents)"))

flattendf1.show()
flattendf1.printSchema()

#ACTORS JSON#





data="""



{

  "country" : "US",

  "version" : "0.6",

  "Actors": [

    {

      "name": "Tom Cruise",

      "age": 56,

      "BornAt": "Syracuse, NY",

      "Birthdate": "July 3, 1962",

      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",

      "wife": null,

      "weight": 67.5,

      "hasChildren": true,

      "hasGreyHair": false,

      "picture": {

                    "large": "https://randomuser.me/api/portraits/men/73.jpg",

                    "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",

                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"

                }

    },

    {

      "name": "Robert Downey Jr.",

      "age": 53,

      "BornAt": "New York City, NY",

      "Birthdate": "April 4, 1965",

      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",

      "wife": "Susan Downey",

      "weight": 77.1,

      "hasChildren": true,

      "hasGreyHair": false,

      "picture": {

                    "large": "https://randomuser.me/api/portraits/men/78.jpg",

                    "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",

                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"

                }

    }

  ]

}



"""



rdd = sc.parallelize([data])





df = spark.read.option("multiline","true").json(rdd)        # DATAFRAME READS



df.show()

df.printSchema()



from pyspark.sql.functions import *



actorsexplode=df.withColumn("Actors",expr("explode(Actors)"))



actorsexplode.show()

actorsexplode.printSchema()











finalflatten = actorsexplode.select(



    "Actors.Birthdate",

    "Actors.BornAt",

    "Actors.age",

    "Actors.hasChildren",

    "Actors.hasGreyHair",

    "Actors.name",

    "Actors.photo",

    "Actors.picture.large",

    "Actors.picture.medium",

    "Actors.picture.thumbnail",

    "Actors.weight",

    "Actors.wife",

    "country",

    "version"





)



finalflatten.show()



finalflatten.printSchema()

## URL DATA FULL CODE##





import os

import urllib.request

import ssl



urldata=(



    urllib.request

    .urlopen(

        "https://randomuser.me/api/0.8/?results=10",

        context=ssl._create_unverified_context()

    )

    .read()

    .decode('utf-8')



)







### PYSPARK







df = spark.read.json(sc.parallelize([urldata]))



df.show()

df.printSchema()





from pyspark.sql.functions import *



resultexp = df.withColumn("results",expr("explode(results)"))



resultexp.show()



resultexp.printSchema()







finalflatten =  resultexp.select(



    "nationality",

    "results.user.cell",

    "results.user.dob",

    "results.user.email",

    "results.user.gender",

    "results.user.location.city",

    "results.user.location.state",

    "results.user.location.street",

    "results.user.location.zip",

    "results.user.md5",

    "results.user.name.first",

    "results.user.name.last",

    "results.user.name.title",

    "results.user.password",

    "results.user.phone",

    "results.user.picture.large",

    "results.user.picture.medium",

    "results.user.picture.thumbnail",

    "results.user.registered",

    "results.user.salt",

    "results.user.sha1",

    "results.user.sha256",

    "results.user.username",

    "seed",

    "version"













)



finalflatten.show()



finalflatten.printSchema()

## URL DATA FULL CODE##





import os

import urllib.request

import ssl



urldata=(



    urllib.request

    .urlopen(

        "https://randomuser.me/api/0.8/?results=10",

        context=ssl._create_unverified_context()

    )

    .read()

    .decode('utf-8')



)







### PYSPARK







df = spark.read.json(sc.parallelize([urldata]))



df.show()

df.printSchema()





from pyspark.sql.functions import *



resultexp = df.withColumn("results",expr("explode(results)"))



resultexp.show()



resultexp.printSchema()







finalflatten =  resultexp.select(



    "nationality",

    "results.user.cell",

    "results.user.dob",

    "results.user.email",

    "results.user.gender",

    "results.user.location.city",

    "results.user.location.state",

    "results.user.location.street",

    "results.user.location.zip",

    "results.user.md5",

    "results.user.name.first",

    "results.user.name.last",

    "results.user.name.title",

    "results.user.password",

    "results.user.phone",

    "results.user.picture.large",

    "results.user.picture.medium",

    "results.user.picture.thumbnail",

    "results.user.registered",

    "results.user.salt",

    "results.user.sha1",

    "results.user.sha256",

    "results.user.username",

    "seed",

    "version"













)



finalflatten.show()



finalflatten.printSchema()



