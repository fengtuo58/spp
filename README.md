# Description

# Utilities for data processing in spark :
aapackage/util_spark.py



X :  RDD, Dataframe or Datasets (all 3 )
     https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html


# Functions are 3 types :

`
Input :
   Kafka streaming -->  X
   SQL     -->   X


Transformer :
      X --> X 
      X --> Pandas Dataframe
      X --> Scipy Sparse Array

            
            
Output :  
   X --> scipy Sparse Array

         https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.save_npz.html
         
   X --> HIVE
   X --> Kafka Streaming
   X --> CSV files
   X --> CouchDB (JSON)
`

#Rules :
   1 file only  util_spark.py
   Functionnal based coding (do not use class when possible):
   Sample usage code should be provided (using data from csv).
   Unit tests code with pytest 



# Infos :
https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
https://stackoverflow.com/questions/31031597/construct-sparse-matrix-on-disk-on-the-fly-in-python
https://stackoverflow.com/questions/40557577/pyspark-sparse-vectors-to-scipy-sparse-matrix






