import findspark
findspark.init()
import pytest
import pyspark
from pyspark import sql
import scipy
from aapackage import util_spark as us
import numpy as np

sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')
###########################################Test case for Hive Table to spark dataframe and rdd##########################

def test_sp_sql_todf():
    df = us.sp_sql_todf(sc, "SELECT * FROM src", "df")
    
    assert isinstance(df, pyspark.sql.dataframe.DataFrame)


def test_sp_sql_tordd():
    rdd = us.sp_sql_todf(sc, "SELECT * FROM src", "rdd")
    assert isinstance(rdd, pyspark.rdd.RDD)

