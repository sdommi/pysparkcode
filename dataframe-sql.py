## SQL PREPARATION
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

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)




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

print("=========DONE GOOD TO GO BELOW======")

print("=========DONE GOOD TO GO BELOW======")
spark.sql("select id from df").show()
spark.sql("describe table df").show()
spark.sql("select id,tdate from df").show()
spark.sql("select * from df where category='Exercise'").show()
spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()
spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
spark.sql("select * from df where product like '%Gymnastics%'").show()
spark.sql("select * from df where category!='Exercise'").show()
spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
spark.sql("select * from df where product is null").show()
spark.sql("select * from df where product is not null").show()
spark.sql("select max(id) from df").show()
spark.sql("select min(id) from df").show()
spark.sql("select count(1) from df").show()
spark.sql("select *,case when spendby='cash' then 1 else 0 end as status from df").show()
spark.sql("select id,category,concat(id,'-',category) as condata from df").show()
spark.sql("select id,category,product,concat_ws('-',id,category,product) from df").show()
spark.sql("select category,lower(category) as lower from df").show()
spark.sql("select category,upper(category) as upper from df").show()
spark.sql("select amount,ceil(amount) as ceil from df").show()  ##ceil is to round up the values to nearest##
spark.sql("select amount,round(amount) from df").show()
spark.sql("select product,coalesce(product,'NA') as nullrep from df").show() ##coalesce is used for replace the values##
spark.sql("select trim(product) from df").show() ##trim is used to remove empty spaces in the end and start##
spark.sql("select distinct category from df").show()
spark.sql("select distinct category,spendby from df").show()
spark.sql("select substring(product,1,10) as sub from df").show()
spark.sql("select category,split(category,' ')[0] as split from df").show()
spark.sql("select * from df union select * from df1").show()
spark.sql("select * from df union all select * from df1").show()
spark.sql("select category,sum(amount) as sum from df group by category").show()
spark.sql("select category,spendby,sum(amount) as sum from df group by category,spendby").show()
spark.sql("select category,spendby,sum(amount) as sum,count(amount) as cnt_amt from df group by category,spendby").show()
spark.sql("select category,max(amount) as max_amt from df group by category").show()
spark.sql("select category,max(amount) as max_amt from df group by category order by category").show()
spark.sql("select category,max(amount) as max_amt from df group by category order by category desc").show()
spark.sql("select category,amount,row_number() over (partition by category order by amount desc) as row_num from df").show()
spark.sql("select category,amount,row_number() over(order by category desc) as row_num from df").show()
spark.sql("select category,amount,dense_rank() over (partition by category order by amount desc) as row_num from df").show()
spark.sql("select category,amount,rank() over (partition by category order by amount desc) as row_num from df").show()
spark.sql("select category,amount,lead(amount) over (partition by category order by amount desc) as row_num from df").show()
spark.sql("select category,amount,lag(amount) over (partition by category order by amount desc) as row_num from df").show()
spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()
spark.sql("select a.*,b.product from df a join df1 b on a.id=b.id").show()
spark.sql("select a.*,b.product from cust a join prod b on a.id=b.id").show()
spark.sql("select a.*,b.product from cust a left join prod b on a.id=b.id").show()
spark.sql("select a.*,b.product from cust a right join prod b on a.id=b.id").show()
spark.sql("select a.*,b.product from cust a full join prod b on a.id=b.id").show()
spark.sql("select a.* from cust a left anti join prod b on a.id=b.id").show()
