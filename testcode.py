from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import next;      from builtins import map
from builtins import zip;       from builtins import str
from builtins import range;     from past.builtins import basestring
from past.utils import old_div; from builtins import object

import os, sys
import datetime, time, arrow,  shutil,  IPython, gc, copy, re
import numexpr as ne, numpy as np, pandas as pd, scipy as sci, tensorflow as tf
from numba import jit, float32

import pyspark
import unittest
import pytest



import util_spark



import findspark
import os
findspark.init()

import pyspark 


sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')
df = us.sp_sql_todf(sc,"SELECT * FROM src")
df.show()

pandas_df = us.sp_df_toPandas_df(df)
print(pandas_df)






















