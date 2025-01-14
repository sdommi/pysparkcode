num=[1,2,3,4,5,6,7,8]
target_sum=12 ###finf two  indexes of numnber their sum =12 o/p:[4,6]

def twonum(num,target_sum):
  for i in range(len(num)):
    for j in range(i+1,len(num)):
      if num[i]+num[j]==target_sum:
        return [i,j]

    
print([1,2,3,4,5,6,7,8,9,10],15)
