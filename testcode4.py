import findspark
findspark.init()
import pyspark
from pyspark import sql
from aapackage import util_spark as us


sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')

sqlContext = sql.SQLContext(sc)

###########################################Test case for spark dataframe to Couch DB##########################
spark_df = sc.parallelize([
    [1,2,3,4],
    [4,5,6,7],
    [7,8,9,8]
]).toDF(["col1","col2","col3","col4"])


us.saveToCouchDb(spark_df,"yaki","http://127.0.0.1:5984")