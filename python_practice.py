"""
How to defined functions and use in the python
"""
from random import random


def find_even_odd(n):
    for i in n:
        if i % 2 == 0:
            print(f"the number: {i} is even")
        else:
            print(f"the number: {i} is odd")

x=[1,2,3,4,5,6,7,8,9,10]
y=[11,12,13,14,15,16,17,18,19,20]
z=[111,121,130,141,150,160,117,118,190,200]

find_even_odd(x)

find_even_odd(y)

find_even_odd(z)

find_even_odd([2])

def conact_string(names):
    for i in names:
        print(i + " reddy")

conact_string(["srini","ram","ravi"])



def mask_email(email):
    return (email[0] + "**********" + email[8:])

def mask_phn(phn):
    return (phn[0:2] + "******" + phn[-3:])

print(mask_email("divyasrini129@gmail.com"))
print(mask_email("srinivasreddy122@gmail.com"))
print(mask_email("bhghcjhcbsreddy122@gmail.com"))

print(mask_phn("6023943859"))
print(mask_phn("614973259"))


my_list = [1, 2, 3, 4, 5, 6, 7, 8]

print(my_list[::3])
print("how to reverse the string")
sentence="hello how are you" ###you are how hello
s1=sentence.split(" ")
###first way
print(" ".join(s1[::-1]))
###second way
for i in s1[::-1]:
    print(i,end="")

print()
def rever_data(words): ###Hello How are you ->[hello, How, are, you]
    return "".join([word[::-1] for word in  words.split(" ")])

print(rever_data("hello how are you"))

print(sentence[::-1])

import itertools

lst = [4, 3, 2, 6, 5, 4, 3, 1]
target_sum = 8

total_comb=[]
for i in range(8):
    for combo in itertools.combinations(lst, 3):
        total_comb.append(combo)
print(total_comb)

# Find all combinations of numbers in the list of all possible lengths
combinations = []
for r in range(1,len(lst)+1):
    for combo in itertools.combinations(lst, r):
        if sum(combo) == target_sum:
            combinations.append(combo)

# Output the results
if combinations:
    print(f"Combinations that sum to {target_sum} are:")
    for combo in combinations:
        print(combo)
else:
    print(f"No combinations found that sum to {target_sum}.")


import random
print(random.randint(10,20))