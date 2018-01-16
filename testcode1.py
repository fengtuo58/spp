import findspark
findspark.init()
import pyspark
from aapackage import util_spark as us

###########################################Test case for Hive Table to spark dataframe##########################
sc = pyspark.SparkContext(master='local[*]', appName='Python Spark SQL Hive integration example')
us.sp_sql_todf(sc,"SELECT * FROM src","df").show()

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