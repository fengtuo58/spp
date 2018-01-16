import findspark
findspark.init()
import pyspark
from pyspark import sql
from aapackage import util_spark as us


sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')

sqlContext = sql.SQLContext(sc)

###########################################Test case for spark dataframe to Scipy Sparse Array##########################
spark_df = sc.parallelize([
    [1,2,3,4],
    [4,5,6,7],
    [7,8,9,8]
]).toDF(["col1","col2","col3","col4"])

sci_csr = us.sp_df_toscimatrix(spark_df)
print(sci_csr)

'''
Output
  (0, 0)	1
  (0, 1)	2
  (0, 2)	3
  (0, 3)	4
  (1, 0)	4
  (1, 1)	5
  (1, 2)	6
  (1, 3)	7
  (2, 0)	7
  (2, 1)	8
  (2, 2)	9
  (2, 3)	8
'''

