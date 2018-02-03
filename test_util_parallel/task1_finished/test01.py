#!/usr/bin/env python
'''
#START_HEADER_SCRIPT  ###################################################################################
# header: parameters for manage.py
NSPLIT=2
ARRAY=[ [1, 2, 3, 4], [2, 3, 4, 5] ]
#DATA_VAR_TO_SPLIT= [  "f1",  "f2", "f3"  ]        # Pandas dataframe or numpy array
#DATA_NSPLIT=   [  1, 3, 4 ]
#END_HEADER_SCRIPT ######################################################################################
'''
import time, sys, os

print("hy msn")
time.sleep(100)
print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
time.sleep(100)
print("qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq")
time.sleep(1)
print("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
time.sleep(1)
print("ccccccccccccccccccccccccccccccc")

for name in os.listdir("task/task_"+str(sys.argv[1])+"/input/"):
    print(name)

time.sleep(5)
