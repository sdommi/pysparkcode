##python variables and types##
myInt = 10
myFloat = 20.5
myComplex = 1j
myString = "divya"
print(myFloat)
print(myString)
print(myInt)
type(myInt)
##python strings and comments
print("hello world")
x = "hi "
y = 'python'
z = ' HELLO, PYTHON '
print(x)
print(y)
print(z)
print(x.capitalize())
print(y.capitalize())
print(x.replace("h","o"))
print(x.upper())
print(z.lower())
print(z.islower())
print(z.isupper())
print(x.isupper())
print(z.split(','))
print(x[0:1])  ##printing based on index
print(z[0:4])
print(y[3:5])
print(z.strip()) ##removes the spaces in the front and rear
print(x*10) ##to print x value for 10 times
#python comments
"""python comments"""
#####boolean comparision operaters and logical operaters
a=10
b=9
print(a>b)
print(a==b)
print(a<b)
print(a!=b)
print(a>=b)
print(a<=b)
print(a>b and a == b)
print(a>b or a<b)
print(not a>b)
###ifelse
x = 100
if x == 100:
    print("x is ", x)
if x>0:
    print("x is +ve")
else:
    print("x is -ve")
if x<0:
    print("x is +ve")
else:
    print("x is -ve")
if x!=100 and x>100:
    print("condition true")
else:
    print("not true")
a = 100
if a != 100:
    print("a is ", a)
########## if else ,elif,nested if else##
name = input("enter name:")
if name == "divya":
    print("name entered is:",name)
elif name == "srini":
    print("name entered is:", name)
elif name == "poushu":
    print("name entered is:",name)
elif name == "vihan":
    print("name entered is:",name)
else:
    print("invalid name entered ")
############################
x = 10
if x >= 0:
    print("x is +ve")
    if (x%2) == 0:
        print("x is even")
    else:
        print("x is odd")
else:
    print("x is -ve")
###################
##y = input("enter y value:")
##if y >= 0:
  ##  print("y is +ve")
    ##if (y%2) == 0:
      #  print("y is even")
    #else:
     #   print("y is odd")
#else:
 #   print("y is -ve")

##########python lists--------->[]########
a = [2,3,4,6]
print("index of a in:" ,a[2])
print("length of a is:",len(a)) ## finding length of list
a.insert(2,8) ##inserting new index value
print("index of a in:" ,a[2])
print(a)
a.remove(8)
print(a)
names = ['poushu','vihaan','divya','srini']
print("index of names in :",names[0])
names.insert(3,'srinivalusu')
print(names[3])
names.remove('srini')
print(names)
names.pop()###pop method removes last values in list
print(names)
x = [7,8,95,5,5]
x.sort() ##sorts values in order
print(x)
x.reverse()
print(x)
print(x.count(5))
x.append(23)
print(x)
y = x.copy()
print(y)
x.clear() ##empty the set
print(x)
####################tuples-------->()##############
x = (1,2,4)
y = (6,7,8)
z = x+y
print(z)
print(z[2])
print(max(z))
a = ('hi',) * 6
print(a)
###############python sets---------->{}############
A = {5,6,7,8}
print(len(A))
A.add(9)
print(A)
A.add(8)  ##sets do not add duplicates
print(A)
A.remove(8) ##removes the number from set and throws error if number is not in list
print(A)
A.discard(7) ##removes the number from set and do not throw error if number not in set
print(A)
A.pop()
print(A)
B = set([76,99,58,58,8,6])
print(B)
print(A.union(B))
print(A.intersection(B))
print(A - B)  ##can also use A.difference(B)
print(B - A)
print(A ^ B)  ##can also use A.symmetric_difference(B)
#########python dictionaries--------->{key,value}#########
n = {'name':'srini','age':50,'year':1997,12:9}
print(n['name'])
print(len(n))
print(n.get('name'))
print(n.pop('name'))
n['name'] = 'divya'
print(n)
############slice and dictionary######
x = [1,2,7,8,3,4]
a = slice(1,3)
print(x[a])
print(x[2:])##prints variables after the given variable
print(x[0:4])##prints from index 0 to 4
print(x[0:5:2]) ##prints indexes of 2 for first 6([0,5]) digits given
print(x[::3])##prints index of 0 and 3
print(x[-1]) ###prints last digit from given list
##########python while loop#######
i = 0
while i<5:
    print("value of i is:",i)
    i+= 1
print("while loop finished")
num = 1
sum = 0
print("plz enter number.to exit enter 0")
while num !=0:
    num = float(input("number?"))
    sum = sum + num;
    print("sum:",sum)
else:
    print("while loop done")
########for loop#############
A = [1,2,3,4]
B = (5,6,7,8)
C = {9,0,1,3}
D = '3456'
E = {'name':'srini', 'age': 45}
for x in A:
    print(x)
for x in B:
    print(x)
for x in C:
    print(x)
for x in D:
    print(x)
##########break and continue######
print("break and continue")
a = [1,2,3,4]
for x in a:
    if x == 3:
        break
    print(x)
i = 0
while i <5:
    print(i)
    i+=1
a = 0
while a <=100:
    if a == 91:
        break
    print(a)
    a+= 1
var=0
while var <=100:
    if var==20:
        var+=1
        continue
    print(var)
    var+=1
#######python function##############
a=int(input("enter a value:"))
b=int(input("enter b value:"))
sum=a+b
print("The sum is :",sum)

def add(var1,var2):
    return var1+var2

c=add(10,20)

print(c)

fullname=add("srini","reddy")
print(fullname)
#############python args####
a=10
print(id(a))



