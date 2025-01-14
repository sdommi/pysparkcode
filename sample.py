print("=========STARTED=========")


a = 2
print(a)

b=3
print(b)

c=a+2
print(c)

d="zeyobron"
print(d)

lisin = [ 1 , 2 , 3 , 4]



rddin = sc.parallelize(lisin)

print("=============RAW RDD=============")
print()
print(rddin.collect())


addin = rddin.map(lambda x : x + 2)
print("=============ADD RDD============")
print()
print(addin.collect())


mulin  = rddin.map(lambda x : x * 10)
print("=============mulin RDD============")
print()
print(mulin.collect())

#   [ 1 , 2 , 3 , 4]

filrdd = rddin.filter(lambda x : x > 2)
print("=============filrdd RDD============")
print()
print(filrdd.collect())