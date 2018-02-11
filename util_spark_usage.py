import findspark
findspark.init()
import pyspark
from pyspark import sql
from aapackage import util_spark as us
import numpy as np

sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')

###########################################Test case for Hive Table to spark dataframe####################################
#us.sp_sql_todf(sc,"SELECT * FROM src","df").show()
'''
+---+-------+
|key|  value|
+---+-------+
|238|val_238|
| 86| val_86|
|311|val_311|
| 27| val_27|
|165|val_165|
|409|val_409|
|255|val_255|
|278|val_278|
| 98| val_98|
|484|val_484|
|265|val_265|
|193|val_193|
|401|val_401|
|150|val_150|
|273|val_273|
|224|val_224|
|369|val_369|
| 66| val_66|
|128|val_128|
|213|val_213|
+---+-------+
'''

###########################################Test case for spark dataframe to Scipy Sparse Array##########################
sqlContext = sql.SQLContext(sc)
row = 400
column= 400
col = []
for i in range(column):
    col.append("col" + str(i))
ll = np.random.randint(5, size=(row, column)).tolist()

spark_df = sc.parallelize(ll).toDF(col)

sci_csr = us.sp_df_toscimatrix(spark_df)
#print(sci_csr)

'''
Output
  (0, 0)	4
  (0, 1)	3
  (0, 2)	3
  (0, 3)	1
  (0, 4)	3
  (0, 6)	2
  (0, 7)	1
  (0, 8)	4
  (0, 9)	4
  (0, 11)	3
  (0, 12)	1
  (0, 14)	3
  (0, 15)	3
  (0, 16)	3
  (0, 17)	2
  (0, 19)	3
  (0, 20)	2
  (0, 21)	4
  (0, 23)	1
  (0, 24)	1
  (0, 25)	3
  (0, 27)	1
'''

###########################################Test case for spark dataframe to Panda datafrmae##########################
panda_df = us.sp_df_to_pandasdf(spark_df)
#print(panda_df)

'''
     col0  col1  col2  col3  col4  col5  col6  col7  col8  col9   ...    \
0       4     0     1     3     1     2     1     3     3     0   ...     
1       1     0     3     3     4     2     1     2     1     1   ...     
2       4     0     3     0     2     0     2     1     3     1   ...     
3       4     0     1     2     4     0     2     0     2     4   ...     
4       1     4     0     4     1     4     1     1     2     1   ...     
5       3     3     4     2     0     3     1     0     0     3   ...     


'''
###########################################Test case for spark dataframe to Couch DB##########################
us.sp_df_tocouchdb(spark_df,"yaki","http://127.0.0.1:5984")



