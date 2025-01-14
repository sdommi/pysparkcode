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

data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
df.show()
###find all participants from both teams##
teamA=df.select("TeamA")
teamB=df.select("TeamB")
#####distinct participants####
dp=teamA.union(teamB).distinct()
dp.show()
winner_count=df.groupby("Won").count()
#####joining both data frames###
final_df = dp.join(winner_count,dp["TeamA"]==winner_count["Won"],"left")
final_df.show()

##drop column:WOn and replace NULL with 0
final_df1=(final_df.drop("Won").fillna(0,"count").\
           withColumnRenamed("count","Won").\
           withColumnRenamed("TeamA","Teamname")).orderBy("Teamname").show()

final_df2=final_df.drop("Won").withColumn("count",expr("case when count is null then 0 else count end")).show()


data1 = [
    (1,"Henry"),
    (2,"Smith"),
    (3,"Hall")
]
data1_schema=["id","name"]
rdd=sc.parallelize(data1,1).toDF(data1_schema)
rdd.show()

data2 = [
    (1,100),
    (2,500),
    (4,1000)
]
data2_schema=["id","salary"]
rdd1=sc.parallelize(data2,1).toDF(data2_schema)
rdd2=rdd1.withColumnRenamed("id","id1")
rdd2.show()
join_data = rdd.join(rdd2,rdd["id"]==rdd2["id1"],"left")
join_data.show()

final_data=join_data.withColumn("salary",expr("case when salary is null then 0 else salary end"))
final_data.drop("id1").orderBy("id").show()

############### Pivot scenario ########################

data = [
    (101,"Eng",90),
    (101,"Sci",80),
    (101,"Mat",95),
    (102,"Eng",75),
    (102,"Sci",85),
    (102,"Mat",90)
]
columns = ["id","subject","marks"]
rdd = sc.parallelize(data,1).toDF(columns)
rdd.show()
pivotdata=rdd.groupBy("id").pivot("subject").agg(first("marks"))
pivotdata.show()

########source to destination scenario#####
data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)]
df=spark.createDataFrame(data,["from","to","distance"])
df.show()
df1=spark.createDataFrame(data,["from1","to1","distance1"])
df1.show()
j1=df.join(df1,(df['from']==df1['to1']) & (df['to']==df1['from1']),'inner')
j1.show()

round_trip=j1.withColumn("Total_round_trip",
               j1['distance']+j1['distance1']).\
               select('from','to','Total_round_trip')
round_trip.show()
from pyspark.sql.window import Window

rn=round_trip.withColumn("row_number",
                      row_number().
                      over(Window.
                       partitionBy("Total_round_trip").
                       orderBy("from"))
                      )
rn.show()
(rn.filter('row_number==1').
 select('from','to','Total_round_trip').show())

## ###################WINDOWING###############################

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

## WINDOWING

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

###################spark interview scenarios########################
""" 
find the the mathing salary with other employess
"""

data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
        ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
        ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
        ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
        ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]

from pyspark.sql.functions import col
df = spark.createDataFrame(data,schema = myschema)
df1 = spark.createDataFrame(data,schema = myschema)
print("******************first way of display***********")
df.createOrReplaceTempView("worktab")
spark.sql(" select a.* from worktab  a join worktab b \
          on (a.workerid != b.workerid) and (a.salary == b.salary)").show()

print("*************Second way to print the result using DSL***************")
matching_sal = df.join(df1,(df["workerid"] != df1["workerid"])
                       & (df["salary"] == df1["salary"]), "inner")
matching_sal.printSchema()

matching_sal.select(df["workerid"],df["firstname"],df["lastname"],df["salary"]).show()

"""
Second scenario
"""
order_data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (3,"1-jan","dispatched"),
    (2, "3-Jan", "shipped")]
order_schema = ["orderid","statusdate","status"]
df = spark.createDataFrame(order_data,schema=order_schema)
print("*****first way using sql******")
df.createOrReplaceTempView("order_data")
spark.sql("select * from order_data where status = 'dispatched' and \
    orderid in(select orderid from order_data where status = 'Ordered') ").show()
print("******second way to display dispatch status*****")
dis_data = df.filter(col("status") == 'dispatched') ###[1,2,3]
ordered_data = (df.filter(col("status") == 'Ordered').
                select("orderid").collect()) ##[1,2]
final_df=dis_data.filter(col("orderid").isin(
   *[row[0] for row in ordered_data]

))

final_df.show()

print("*****3rd scenario")
data = [(1111, "2021-01-15", 10),
        (1111, "2021-01-16", 15),
        (1111, "2021-01-17", 30),
        (1112, "2021-01-15", 10),
        (1112, "2021-01-15", 20),
        (1112, "2021-01-15", 30)]

myschema = ["sensorid", "timestamp", "values"]
df = spark.createDataFrame(data,schema = myschema)
df.createOrReplaceTempView("Ldata")
print("**first way using sql**")
df2=spark.sql(
    "select *,lead(values,1,null) \
    over (partition By sensorid order By timestamp) as l_values from Ldata"
).show()
spark.sql("with CTE as(select *,lead(values,1,null) \
        over (partition By sensorid order By timestamp) as l_values from Ldata \
) select *,l_values-values as new_values from CTE where l_values is not null").show()
print("****dsl way**")

from pyspark.sql.functions import *
from pyspark.sql.window import Window
lead_values = df.withColumn("lvalues",lead("values",1).
               over(Window.partitionBy("sensorid").orderBy("timestamp")))
lead_values.show()
print("*****filter null values******")
filter_null=lead_values.filter(col("lvalues").isNotNull())
filter_null.show()

print("******write expression subtract values from lead values****")

diff_values=filter_null.withColumn("f_values",expr("lvalues -values"))
diff_values.show()
print("**drop unnecessary columns*******")
diff_values.select("sensorid","timestamp","values","f_values").show()


"""
(Write a query to list the unique customer names in the custtab table,
 along with the number of addresses associated with each customer.)
"""
cust_data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
cust_schema = ["custid", "custname", "address"]
df = spark.createDataFrame(cust_data,schema=cust_schema)
df.createOrReplaceTempView("cust_table")
print("**sql way**")
cust_list=spark.sql("select custid,custname,collect_set(address) as set_addresses \
from cust_table group By custid,custname")
cust_list.show()

print("*********dsl way**********")
cust_df = (df.groupBy("custid","custname").agg(collect_set("address").alias("addresses"))
           .select("custid","custname","addresses").orderBy("custid"))
cust_df.show()

"""
scenario 5:
Read data from above file into dataframes(df1 and df2).
Display number of partitions in df1.
Create a new dataframe df3 from df1, along with a new column salary, and keep it constant 1000
append df2 and df3, and form df4
Remove records which have invalid email from df4, emails with @ are considered to be valid.
Write df4 to a target location, by partitioning on salary.

"""

data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1,schema = myschema1)

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2,schema = myschema2)

df1.createOrReplaceTempView("data_1")
df2.createOrReplaceTempView("data_2")
print("***dsl way**")
print("The no of partitions in df1 is", df1.rdd.getNumPartitions())
df3 = df1.withColumn("salary",lit(1000))
df3.show()
print("**appending df2 and df3**")
df4=df3.union(df2).filter(col("email").rlike("@"))
df4.show()
print("***sql way***")
df = spark.sql("""with CTE as(select *,1000 as salary from data_1
              union select * from data_2)select * from CTE where email like '%@%'
              """ )
df.show()
"""
scenario:6
(For Employee salary greater than 10000 give designation as manager else employee)
"""
data = [
    ("1", "a", "10000"),
    ("2", "b", "5000"),
    ("3", "c", "15000"),
    ("4", "d", "25000"),
    ("5", "e", "50000"),
    ("6", "f", "7000")
]
myschema = ["empid","name","salary"]
df1 = spark.createDataFrame(data,schema = myschema)
df1.createOrReplaceTempView("emp_data")
print("***sql way**")
emp_data=spark.sql("""select *,
          case
             when salary >= 10000 then "manager"
             else
             "employee"
          end as designation from emp_data
          """)
emp_data.show()
print("***dsl way***")
df1.withColumn("designation",
               expr("case when salary >= 10000 then 'manager' \
               else 'employee' \
               end")).show()
"""scenario:7"""
data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]
myschema = ["sale_id", "product_id", "year", "quantity", "price"]
df = spark.createDataFrame(data,schema = myschema)
df.createOrReplaceTempView("data")
print("***sql way**")
f_data=spark.sql("""with CTE as(select *,dense_rank() over(partition By
 year order By quantity desc)
           as rnk from data) select * from CTE where rnk = 1""")
f_data.show()
print("**dsl way**")
data1=df.withColumn("rnk",dense_rank().over(Window.partitionBy("year")
                         .orderBy(desc("quantity")))).filter(col("rnk")==1)
data1.show()

"""
scenario:8
"""
data = [
    ("India",),
    ("Pakistan",),
    ("SriLanka",)
]
myschema = ["teams"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("cric_team")
print("**sql way**")
team=spark.sql("""select concat(a.teams, ' vs ' ,b.teams) as matches 
from cric_team a 
join  cric_team b on a.teams < b.teams""")
team.show()
print("**dsl way**")
df1=spark.createDataFrame(data,schema=myschema).withColumnRenamed("teams","teams1")
df.printSchema()
teams_df=df.join(df1,df["teams"] < df1["teams1"],"inner").\
  withColumn("matches",concat(df["teams"], lit(" vs "),df1["teams1"]))

teams_df.select("matches").show()

"""
scenario 9:
"""
data = [
    ("a", [1, 1, 1, 3]),
    ("b", [1, 2, 3, 4]),
    ("c", [1, 1, 1, 1, 4]),
    ("d", [3])
]
df = spark.createDataFrame(data, ["name", "rank"])
df.createOrReplaceTempView("r_data")
print("**sql way**")
fd=spark.sql("select name from r_data where name like 'c'")
fd.show()
print("**dsl way**")
df.filter(col("name") == "c").select("name").show()
""" 
scenario 10:
"""
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
myschema=["empid", "commissionamt", "monthlastdate"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("data")
print("**sql way**")
fd=spark.sql("""(select empid,monthlastdate,commissionamt
             from data where empid=1  and commissionamt=
             (select min(commissionamt) from data))
             union all
             (select empid,monthlastdate,commissionamt
             from data where empid=2  and commissionamt=
             (select max(commissionamt) from data)) 
             """)
fd.show()
print("**dsl way**")
df1 = df.filter(col("empid")==1).agg(min("commissionamt").alias("min_cms_amt"))
df1.show()

#### Get the column 'name' from df as a list....very very imp
print(df1.rdd.flatMap(lambda x:x).collect()) ###o/p:[200]
#### Get the list of names from df..v.v. imp
[print(row['min_cms_amt']) for row in df1.collect()] ##o/p:200
df2 = df.filter(col("empid")==2).agg(max("commissionamt").alias("max_cms_amt"))
df2.show()
e_1=df.filter((df["empid"]==1) & (df["commissionamt"].
        isin(df1.rdd.flatMap(lambda x:x).collect())
))

e_2=df.filter((df["empid"]==2) & (df["commissionamt"].
            isin(df2.rdd.flatMap(lambda x:x).collect())
                                  ))
final_df=e_1.unionAll(e_2)
e_1.show()
e_2.show()
final_df.show()
"""scenario:11
(I have a table called Emp_table, it has 3 columns, Emp name, emp ID , salary
in this I want to get salaries that are >10000 as Grade A, 5000-10000 as grade B and < 5000 as Grade C, write an SQL query)"""
data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]
myschema=["emp_id", "emp_name", "salary"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("emp_table")
print("***sql way**")
emp_sal=spark.sql("""select *,case
when salary > 10000 then 'Grade A'
when salary between 5000 and 10000 then 'Grade B'
else 'Grade C'
end as grades from emp_table
""")
emp_sal.show()
print("**dsl way**")
df.withColumn("Grades",expr("case when salary > 10000 then 'Grade A' "
                       "when salary between 5000 and 10000 then 'Grade B'"
                       "else 'Grade  C' end")).show()
"""
scenario:12
"""
data=[
    ("Renuka1992@gmail.com", "9856765434"),
    ("anbu.arasu@gmail.com", "9844567788")
]
myschema=["email", "mobile"]
df = spark.createDataFrame(data,schema=myschema)
def myemail(email):
    return (email[0] + "**********" + email[8:])

#creating UDF functions for masked data, here mobile[0:2] is it will take string from Index 0 to 2 letters and mobile[-3:] is it will take string last three index to end the end of the string
def mymobile(mobile):
    return (mobile[0:2] + "*****" + mobile[-3:])

def extended_mob(ph):
    return(ph + "999")


df.withColumn("email",udf(myemail)(df.email)).\
    withColumn("mobile1",udf(mymobile)(df.mobile)).\
withColumn("extended_mph",udf(extended_mob)(df.mobile)).show()


"""
scenario:13
We have employee id,employee name, department. Need count of every department employees
"""
data = [(1, "Jhon", "Development"),
        (2, "Tim", "Development"),
        (3, "David", "Testing"),
        (4, "Sam", "Testing"),
        (5, "Green", "Testing"),
        (6, "Miller", "Production"),
        (7, "Brevis", "Production"),
        (8, "Warner", "Production"),
        (9, "Salt", "Production")]
myschema=["emp_id", "emp_name", "dept"]
df = spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("dept_data")
print("**sql way**")
c_dept=spark.sql("select dept,count(dept) from dept_data group By dept")
c_dept.show()
print("dsl way")
c_d=df.groupBy("dept").agg(count("dept").alias("dept_count"))
c_d.show()
"""
scenario:14
"""
data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]
myschema=["rollno","name", "telugu", "english", "maths", "science", "social"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("score")
print("**dsl way**")
fd=df.withColumn("total",expr('telugu+english+maths+science+social'))
fd.show()
print("**sql way**")
fd1=spark.sql("select *,(telugu + english + maths + science + social) as total from score")
fd1.show()
"""
scenario: 15
"""
l1 = [2, 3, 4, 5]
l2 = [6, 7, 8, 9]
print("**dsl way**")
fd=l1.append(l2)
print(l1)
"""
scenario:16
"""
data = [(1, "Jhon", "Testing", 5000),
        (2, "Tim", "Development", 6000),
        (3, "Jhon", "Development", 5000),
        (4, "Sky", "Prodcution", 8000)]
myschema=["id", "name", "dept", "salary"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("d_data")
print("**sql way**")
df1=spark.sql("""with CTE (select id,name,dept,salary,
             row_number() over(partition by name order by id) as rn
             from d_data) select id,name,dept,salary from CTE where rn=1 order by id""")
df1.show()
print("**dsl way**")
(df.withColumn("rn",row_number().
              over(Window.partitionBy("name").orderBy("id"))).
 filter(col("rn") == 1).show())
"""
scenario:17
"""
data = [(1, "Tim", 24, "Kerala", "India"),
        (2, "Asman", 26, "Kerala", "India")]
myschema=["emp_id", "name", "age", "state", "country"]
df=spark.createDataFrame(data,schema=myschema)
df.createOrReplaceTempView("data")
data2 = [(1, "Tim", 24, "Comcity"),
         (2, "Asman", 26, "bimcity")]
myschema=["emp_id", "name", "age", "address"]
df1=spark.createDataFrame(data2,schema=myschema)
df1.createOrReplaceTempView("data2")
print("***sql way***")
df3=spark.sql("""select d1.emp_id,
                d1.name,d1.age,d1.state,d1.country,d2.address
                from data as d1 inner join data2 as d2 
              on d1.emp_id = d2.emp_id""")
df3.show()
print("***dsl way***")
print("if we use DSL,catalyst optimizer will optimize the code before execute:\
      it will create a plan")
df3=df.join(df1,"emp_id","inner").select(df["emp_id"],df["name"],
        df["age"],df["state"],df["country"],df1["address"])
df3.show()
"""
scenario:18:  print the given string into reverse order
data =[("The Social Dilemma")]
"""
def rever_data(words): ###Hello How are you ->[hello, How, are, you]
    return "".join([word[::-1] for word in  words.split(" ")])
sentence="The Social Dilemma" ###you are how hello
s1=sentence.split(" ")
print(rever_data("The Social Dilemma"))
print(sentence[::-1])


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
 .partitionBy("id")
 .save("fruits"))

