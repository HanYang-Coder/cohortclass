from mapreduce import *

# fix path if needed
import os
print(os.getcwd())
dir = 'C:/Documents/SUTD/Term 5/Database/cohortclass/cc10/ex2/'

def read_db(filename):
    db = []
    with open(filename, 'r') as f:
        for l in f:
            db.append(l)
    f.close()
    return db
            
test_db = read_db(dir + "data/price.csv")

# TODO: FIXME
# the result should contain a list of suppliers, 
# with the average sale price for all items by this supplier.

# result = []

# mapper:
# input: String, pId, sId, price 1,6,997,70
# output: (supplier, price) pairs
def mapper(line):
    # print(line)
    arr=line.strip().split(',')
    pId, sId, price = arr
    return (sId, float(price))

# reducer: 
def reducer(p):
    # print(p)
    sId, price_list = p
    avg_price = sum(price_list) / len(price_list)

    return (sId, avg_price)

mapper_out  = map(mapper, test_db)
# you can only iterate an iterator once
# print(list(mapper_out))
print(mapper_out)
result = reduceByKey2(reducer,mapper_out)

# print the results
for supplier,avg_price in result:
    print(supplier, avg_price)








## mini discussion answer

##  for aggregiation that can be defined using a commutative and associative binary operation, we should use reducebykey2, e.g. calculating median