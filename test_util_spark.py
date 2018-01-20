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




#########################################Creation of DataFrame #########################################################

sqlContext = sql.SQLContext(sc)
row = 400
column= 400
col = []
for i in range(column):
    col.append("col" + str(i))
ll = np.random.randint(5, size=(row, column)).tolist()

spark_df = sc.parallelize(ll).toDF(col)


###########################################Test case for spark dataframe to Scipy Sparse Array##########################

def test_sp_df_toscimatrix():
    sci_csr = us.sp_df_toscimatrix(spark_df)
    assert isinstance(sci_csr, scipy.sparse.csr.csr_matrix)

###########################################Test case for spark dataframe to Panda datafrmae#############################

def test_sp_df_to_pandasdf():
    pd_df = us.sp_df_to_pandasdf(spark_df)
    assert isinstance(pd_df, pandas.core.frame.DataFrame)

###########################################Test case for spark dataframe to Couch DB##########################
#us.sp_df_tocouchdb(spark_df,"yaki","http://127.0.0.1:5984")















