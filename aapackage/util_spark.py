# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import next;      from builtins import map
from builtins import zip;       from builtins import str
from builtins import range;     from past.builtins import basestring
from past.utils import old_div; from builtins import object

import os, sys
import datetime, time, arrow,  shutil,  IPython, gc, copy, re, argparse
import numexpr as ne, numpy as np, pandas as pd, scipy as sci, tensorflow as tf
from numba import jit, float32
from attrdict import AttrDict as dict2


################################################################################################################
### Need to create ENV variable  CONFIGMY_ROOT_FILE = YourFOlder/CONFIGMY_ROOT_FILE.py
'''
#  encoding=utf-8
#  Need to create ENV variable  CONFIGMY_ROOT_FILE = YourFOlder/CONFIGMY_ROOT_FILE.py
#  CONFIGMY_ROOT_FILE.py     OS_Name + username
#  DIRCWD is root folder of your project

{
 "win+asus1": {  # windows platform + asus1 user
   "DIRCWD" :          "D:/_devs/Python01/project27/",

   "github_login" :    "",
   "github_pass" :     "",
   "aws_login"    :     "",
   "aws_password" :     "",
   "EC2CWD"       :     "/home/ubuntu/notebook/"
},
               

 "lin+ubuntu": {
   "DIRCWD" :     "/home/ubuntu/project27/",
   "conda_env":   ["tf_gpu_12", "root"],
   "github_login" :    "",
   "github_pass" :     "",

},


}
'''
#############################################################################################################
'''
try :
 import configmy; CFG, DIRCWD= configmy.get(config_file="_ROOT", output= ["_CFG", "DIRCWD"])
 os.chdir(DIRCWD); sys.path.append(DIRCWD + '/aapackage')
except :
 DIRCWD= 'your folder '
 os.chdir(DIRCWD); sys.path.append(DIRCWD + '/aapackage')



__path__=     DIRCWD +'/aapackage/'
__version__=  "1.0.0"
__file__=     "util_spark.py"

'''
#############################################################################################################





########### Pandas functions #################################################################################
from attrdict import AttrDict as dict2 ; from collections import defaultdict
from numba import njit
from itertools import combinations
from collections import OrderedDict
from sklearn.cross_validation import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.preprocessing import OneHotEncoder
from scipy.io import mmread

@njit
def findfirst(array, item):
    for i, v in enumerate(array):
        if v == item: return i
    return -1



def csv_to_1hot_sparse(file_csv, dtypes=['int64'], colname=['user_id'],  file_output="", sep=" ") :
   '''
   :param csv_file:       read into pandas
   :param colname:        only some columns, but full row
   :param file_output:     spi.mmwrite( "../Data/Validation/user_feat_mtrx_.mtx") 
   :return:    None or matrix
   '''
   #only 1 column reading
   df  = pd.read_csv(file_csv, sep=" ", colname=colname, skipinitialspace=True, usecols=colname)
   df  = cat_reindex_1toN(df, colname)
   sp1 = pd_to_onehotsparse(df, colcat=colname, onehotfit=None)

   if file_output == "" : return sp1
   else                 : sp1.mmwrite( file_output)



def sp_matrix_merge(file_sp1, file_sp2, file_output=""):
   sp1 = mmread( file_sp1)
   sp2 = mmread( file_sp2)
   sp0 = sci.sparse.hstack((sp1, sp2))
   sp0 = sp0.tocsr()
   if file_output == "" : return sp0
   else :  sp1.mmwrite( file_output)




#### remap  ---> 1..N   ###################################################################################
def cat_reindex_1toN(df, colcat, return_catsize=0)  :
  for x in colcat :
    llx   = df[x].unique()
    try :
      df[x] = df[x].apply( lambda t : findfirst(llx ,  t) )
    except :  # String values
      llx= list(llx)
      df[x] = df[x].apply( lambda t : llx.index(t) )

  ncat_list = []
  for x in colcat :
      ncat_list.append( df[x].max() + 1 )  #total features per columns + numerical

  print("N Binary features :", sum(ncat_list))
  if return_catsize :  return df, ncat_list
  else  :              ncat_list




##########################################################################################################
################### For TFFM input feed   ################################################################
def pd_to_onehotsparse(df, colcat, colnum=None,  onehotfit=None, onehotype='float32' ) :
  ''' Pandqs to scipy csr for TFFm, Fast FM factorization machines
  '''
  if onehotfit is None :
      onehot   = OneHotEncoder(sparse=True, dtype=onehotype)
      onehotfit= onehot.fit(df[colcat])

  Mcat =  onehotfit.transform(df[colcat])
  if colnum is None :
    Mcat =  Mcat.tocsr()
    return Mcat
  else :
    Mnum =  df[colnum].to_sparse().to_coo()  # .tocsr()
    Mall =  sci.sparse.hstack((Mnum, Mcat))
    Mall =  Mall.tocsr()
    return Mall




#################### Spark functions   ###################################################################
# sc: sparkcontext
Sparkcontext = None
import pyspark
from pyspark.sql import SparkSession



def zdoc():
  print(
  '''
https://boazmohar.github.io/pySparkUtils/pySparkUtils.html#module-pySparkUtils.utils

http://deelesh.github.io/pyspark-windows.html
https://triamus.github.io/post/2017-09-22-install-spark-on-windows/


https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0


https://hioptimus.com/

https://www.cloudera.com/documentation/enterprise/5-9-x/topics/spark_python.html


https://pypi.python.org/pypi/isparkcache/0.1.12

https://pypi.python.org/pypi/dummy_spark/0.0.1

https://pypi.python.org/pypi/sparkly/2.3.0

https://pypi.python.org/pypi/pyspark_db_utils/0.0.1

https://docs.databricks.com/spark/latest/data-sources/zip-files.html

https://gist.github.com/search?p=3&q=pyspark&ref=searchresults&utf8=%E2%9C%93



  ''' )
 
 
 
 

def sp_file_tohive(sc, filename='' , dbname, sql) :
   ''' local binary file to hive file

   '''




def sp_hive_tomemory(sc, filename='' , dbname, sql) :
   ''' local binary file to hive file

   '''

   


   
def sp_df_tocsv(sc, df, filename) :
   ''' Spark dataframe to local csv
       Issues with driver memory

   '''
   




def sp_sql_todf(sc, sql='', outype='df/dset/rdd') :
    spark = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()
    
    if outype = 'df'   :    spark_df = spark.sql(sql)
    if outype = 'dset' :  
    if outype = 'rdd'  :    
        
    return spark_df




def sp_df_to_pandasdf(df):
  '''
     Issue if driver memory < distributed memory
  
  '''
  return df.toPandas()




def sp_df_tosql(sc, dbname, sql='') :
   ''' Spark dataframe to HIVE SQL  
 

   '''





def sp_df_toscimatrix(sc= Sparkcontext, df=None, nsplit=5) :
   '''  Spark dataframe to Scipy Matrix
        numpy Matrix[ u(i), h(j) ] = 1   if     df : shape =  (100000, 2)  ['user', 'item' ]   
         
         
        Matrix is split into 5 components if very large. 
         
   '''
   










 
 










###############################################################################################################################
def py_exception_print():
    import linecache
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))



def py_log_write(LOGFILE, prefix):
 import arrow, os
 ###########################################################################################################################
 #LOGFILE =     DIRCWD + '/aapackage/'+ 'ztest_all.txt';
 print(LOGFILE)
 DATENOW =     arrow.utcnow().to('Japan').format("YYYYMMDDHHmm")
 UNIQUE_ID=    prefix +"_"+ DATENOW +"_"+ str(np.random.randint(10**5, 10**6,  dtype='int64'))
 sys.stdout =  open( LOGFILE, 'a')
 print("\n\n"+UNIQUE_ID+" ###################### Start:" + arrow.utcnow().to('Japan').format()  + "###########################") ; sys.stdout.flush() ; print(os)
 return UNIQUE_ID
 ###########################################################################################################################





####################################################################################################################
############################ UNIT TEST #############################################################################
if __name__ == '__main__' :
  import argparse;  ppa = argparse.ArgumentParser()       # Command Line input
  ppa.add_argument('--do', type=str, default= 'action',  help='test / test02')
  arg = ppa.parse_args()


if __name__ == '__main__' and arg.do == "test":
 print(__file__)
 try:
   UNIQUE_ID = py_log_write( DIRCWD + '/aapackage/ztest_log_all.txt', "util")

   #################################################################################################################
   import numpy as np, pandas as pd, scipy as sci

   vv  =   np.random.rand(1,10)
   mm  =   np.random.rand(100,5)
   df1  =  pd.DataFrame(mm, columns=["aa", "bb", 'c', 'd', 'e'] )
 except  Exception as err:
     print(err)


















'''

#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from test_support.smvbasetest import SmvBaseTest
from smv import SmvCsvFile

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, struct, sum

class GroupedDataTest(SmvBaseTest):
    def test_smvFillNullWithPrevValue(self):
        df = self.createDF("k:String; t:Integer; v:String", "a,1,;a,2,a;a,3,b;a,4,")
        res = df.smvGroupBy("k").smvFillNullWithPrevValue(col("t").asc())("v")
        expect = self.createDF("k:String; t:Integer; v:String",
              """a,1,;
                 a,2,a;
                 a,3,b;
                 a,4,b"""
        )
        self.should_be_same(expect, res)

    def test_smvPivot_smvPivotSum(self):
        df = self.createDF("id:String;month:String;product:String;count:Integer", "1,5/14,A,100;1,6/14,B,200;1,5/14,B,300")
        r1 = df.smvGroupBy('id').smvPivot([['month', 'product']],['count'],["5_14_A", "5_14_B", "6_14_A", "6_14_B"])
        r2 = df.smvGroupBy('id').smvPivotSum([['month', 'product']],['count'],["5_14_A", "5_14_B", "6_14_A", "6_14_B"])

        e1 = self.createDF("id: String;count_5_14_A: Integer;count_5_14_B: Integer;count_6_14_A: Integer;count_6_14_B: Integer",
                            """1,100,,,;
                               1,,,,200;
                               1,,300,,""")
        e2 = self.createDF("id: String;count_5_14_A: Long;count_5_14_B: Long;count_6_14_A: Long;count_6_14_B: Long",
                           "1,100,300,0,200")
        self.should_be_same(r1, e1)
        self.should_be_same(r2, e2)

    def test_smvPivotCoalesce(self):
        df = self.createDF("k:String; p:String; v:Integer", "a,c,1;a,d,2;a,e,;a,f,5")
        res = df.smvGroupBy("k").smvPivotCoalesce(
            [['p']],
            ['v'],
            ['c', 'd', 'e', 'f']
        )
        expect = self.createDF("k: String;v_c: Integer;v_d: Integer;v_e: Integer;v_f: Integer",
            "a,1,2,,5"
        )
        self.should_be_same(expect, res)

    def test_smvTimePanelAgg(self):
        df = self.createDF("k:Integer; ts:String; v:Double",
            """1,20120101,1.5;
                1,20120301,4.5;
                1,20120701,7.5;
                1,20120501,2.45"""
            ).withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        import smv.panel as p

        res = df.smvGroupBy('k').smvTimePanelAgg(
            'ts', p.Quarter(2012,1), p.Quarter(2012,2)
        )(
            sum('v').alias('v')
        )

        expect = self.createDF("k: Integer;smvTime: String;v: Double",
                """1,Q201201,6.0;
                    1,Q201202,2.45""")

        self.should_be_same(expect, res)

    def test_smvTimePanelAgg_with_Week(self):
        df = self.createDF("k:Integer; ts:String; v:Double",
                 "1,20120301,1.5;" +
                 "1,20120304,4.5;" +
                 "1,20120308,7.5;" +
                 "1,20120309,2.45"
             ).withColumn("ts", col('ts').smvStrToTimestamp("yyyyMMdd"))

        import smv.panel as p

        res = df.smvGroupBy('k').smvTimePanelAgg(
            'ts', p.Week(2012, 3, 1), p.Week(2012, 3, 10)
        )(
            sum('v').alias('v')
        )

        expect = self.createDF("k: Integer;smvTime: String;v: Double",
            """1,W20120305,9.95;
                1,W20120227,6.0""")

        self.should_be_same(res, expect)

    def test_smvPercentRank(self):
        df = self.createDF("id:String;v:Integer","a,1;a,;a,4;a,1;a,1;a,2;a,;a,5")
        res = df.smvGroupBy('id').smvPercentRank(['v'])

        exp = self.createDF("id: String;v: Integer;v_pctrnk: Double",
                            """a,,;
                            a,,;
                            a,1,0.0;
                            a,1,0.0;
                            a,1,0.0;
                            a,2,0.6;
                            a,4,0.7999999999999999;
                            a,5,1.0""")

        self.should_be_same(res, exp)

    def test_smvQuantile(self):
        df = self.createDF("id:String;v1:Integer;v2:Double","a,1,1.0;a,,2.0;a,4,;a,1,1.1;a,1,2.3;a,2,5.0;a,,3.1;a,5,1.2")
        res = df.smvGroupBy("id").smvQuantile(["v1", "v2"], 4)

        exp = self.createDF("id: String;v1: Integer;v2: Double;v1_quantile: Integer;v2_quantile: Integer",
                            """a,,2.0,,3;
                            a,,3.1,,4;
                            a,1,1.0,1,1;
                            a,1,1.1,1,1;
                            a,1,2.3,1,3;
                            a,2,5.0,3,4;
                            a,4,,4,;
                            a,5,1.2,4,2""")
        self.should_be_same(res, exp)



'''