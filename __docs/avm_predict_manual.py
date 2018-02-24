# -*- coding: utf-8 -*-
# %load_ext autoreload
# %autoreload
# import os, sys
# DIRCWD=  'G:/_devs/project27/' if sys.platform.find('win')> -1   else  '/home/ubuntu/notebook/' if os.environ['HOME'].find('ubuntu')>-1 else '/media/sf_project27/'
#  os.chdir(DIRCWD); sys.path.append(DIRCWD + '/aapackage'); # sys.path.append(DIRCWD + '/linux/aapackage')
# execfile( DIRCWD + '/aapackage/allmodule.py')
# import util, 
import copy, numpy as np,  pandas as pd,arrow
from attrdict import AttrDict as dict2; from collections import defaultdict
import sys,os

############################################################################################

######################### FOLDERS ##########################################################
# execfile( DIRCWD + '/aapackage/coke_functions.py')
# execfile( DIRCWD + '/unerry/cc_folders.py')




if __name__ == "__main__"  :
 import argparse
 ppa = argparse.ArgumentParser()
 ppa.add_argument('--folder', type=str, default= ''  ,    help=" unit_test")
 ppa.add_argument('--input_file', type=str, default= '',  help=" unit_test")
 ppa.add_argument('--date_start', type=str, default= '',  help=" date start")
 arg = ppa.parse_args()
 
 input_file= arg.input_file
 folder=     arg.folder
 date_start= int(arg.date_start) 
 
 if input_file == '':
   print( """ 
       usage: avm_predict [-h] [--folder FOLDER] [--input_file INPUT_FILE]
       [--date_start DATE_START]   
   """)
   
   sys.exit()


else :
 input_file= "input_data4.csv"

 folder=  "E:/Dropbox/__data"

 date_start= 20160701  





############################################################################################
############################################################################################






#### Past Values  ##########################################################################
def weekday_excel(x) :
 wday= arrow.get( str(x) , "YYYYMMDD").isocalendar()[2]    
 if wday != 7 : return wday+1
 else :    return 1
 

def weekyear_excel(x) :     
 dd= arrow.get( str(x) , "YYYYMMDD")
 wk1= dd.isocalendar()[1]

 # Excel Convention
 # dd0= arrow.get(  str(dd.year) + "0101", "YYYYMMDD")
 dd0_weekday= weekday_excel( dd.year *10000 + 101  )
 dd_limit= dd.year*10000 + 100 + (7-dd0_weekday+1) +1

 ddr= arrow.get( str(dd.year) + "0101" , "YYYYMMDD")
 
 # print dd_limit

 if    int(x) < dd_limit :
    return 1
 else :    
     wk2= 2 + int(((dd-ddr ).days  - (7-dd0_weekday +1 ) )   /7.0 ) 
     return wk2    
 

def get_ma1(d0, coeff=[1.0, 1.0, 1.0], debug=0) :
     dd=  arrow.get( str(d0) , "YYYYMMDD")
     # dd_int =int(dd)
     weekyear=  weekyear_excel(d0)
     weekday=   weekday_excel(d0)
     if debug: print (weekyear, weekday)
     nn=0

     c1=0.0
     #if date_start < 20150101    :
     try : 
      c1= df[ (df.year==2014 ) & (df.weekyear==weekyear) & (df.weekday==weekday) ].ma1.values[0]
     except :
      c1= 0.0  
     nn= nn+1 if c1 != 0 else 0
    
     #c2=0.0
     # if date_start < 20160101    :
     try : 
      c2= df[ (df.year==2015 ) & (df.weekyear==weekyear) & (df.weekday==weekday) ].ma1.values[0]
     except :
      try :
         c2= df[ (df.year==2015 ) & (df.weekyear==weekyear-1) & (df.weekday==weekday) ].ma1.values[0]
      except :
         c2= 0.0 
     nn= nn+1 if c2 != 0.0 else 0

     c3=0.0
     # if date_start < 20170101    :     
     try : 
      c3= df[ (df.year==2016 ) & (df.weekyear==weekyear) & (df.weekday==weekday) ].ma1.values[0]
     except :
      try :
         c3= df[ (df.year==2016 ) & (df.weekyear==weekyear-1) & (df.weekday==weekday) ].ma1.values[0]
      except :
         c3= 0.0 
     nn= nn+1 if c3 != 0.0 else 0


     try : 
      c00= df[ (df.year==2017 ) & (df.weekyear==weekyear) & (df.weekday==weekday) ].ma1.values[0]
     except :
      try :
         c00= df[ (df.year==2017 ) & (df.weekyear==weekyear-1) & (df.weekday==weekday) ].ma1.values[0]
      except :
         c00= 0.0 
    
     if debug: print(c00, c1, c2, c3)
    
     if nn > 0 :
      ma1_avg= 1 / (nn+0.000001) *(  coeff[0] * c1 +  coeff[1] * c2 + coeff[2] * c3 )
     else :
      ma1_avg= 0.0
     return ma1_avg

    




#####################################################################################
# dfa= pd.read_csv( folder + "/" + input_file  ).fillna(0.0)

dfall= pd.read_csv( "E:/Dropbox/pred_details/vm_50_list_skugroup.csv"  ).fillna(0.0)
dfall= dfall[ dfall.productsegment.isin(['COFFEE', ' COFFEE-BLACK', 'COFFEE-CAFEOLAIT', 'COFFEE-SUGAR', 'SPORTS', 'SSD', 'TEA','WATER']) ]   
ll1 = dfall[[ "machine_code", "productsegment" ]].drop_duplicates().values


date_start= 20160619

for  ii,x in enumerate(ll1) :
 dfa= copy.deepcopy(dfall[ (dfall.machine_code== x[0] ) & (dfall.productsegment== x[1] ) ])


 ####Date Transformation   #####################################################
 dfa["month"]=    dfa["date"].apply( lambda x :  int(str(x)[4:6]) ).fillna(0).astype("int16")
 dfa["year"]=     dfa["date"].apply( lambda x :  int(str(x)[0:4]) ).fillna(0).astype("int16")

 dfa["weekyear"]= dfa["date"].apply( lambda x : weekyear_excel(x)  )
 dfa["weekday"]=  dfa["date"].apply( lambda x : weekday_excel(x)  )

 dfa["dayyear"]  = dfa.apply( lambda x : 7*(x["weekyear"]-1) + x["weekday"] , axis=1)




 ####Add intermerdiate results   ###############################################
 dfa["ma20"] = dfa["sales"].shift(1).rolling(window=20  ).mean().fillna(0)


 dfa["ma0"] = dfa["sales"] - dfa["ma20"]


 #### ddof=1 : N-1    or   ddof= 0  Divides by N
 dfa["ma0_stdev"] = dfa["ma0"].shift(1).rolling(window=12  ).std(ddof=0).fillna(0)

 dfa["ma1"]=   dfa["ma0"]  /  dfa["ma0_stdev"] 



 ### Prediction  ####################################################################
 ma1_past= dfa.groupby([ "year", "weekyear", "weekday"  ]).agg({"ma1": "mean"}).reset_index()
 ma1_past = ma1_past.replace([np.inf, -np.inf], 0.0)

 ma1_past["idx"]= ma1_past.apply(lambda x: x["year"]*10000 + x["weekyear"]*100 + x["weekday"], axis=1 ).astype("int")
 df= copy.deepcopy( ma1_past )

 df= df[df.idx > 20144301]




 dfa["ma1_predict"]= dfa.apply( lambda x : get_ma1(x["date"]), axis=1)

 dfa["predict"]= dfa["ma0_stdev"]  * dfa["ma1_predict"] + dfa["ma20"]



 dfb= copy.deepcopy(dfa[ dfa.date > date_start ])
 dfb= dfb.groupby([ "machine_code", "productsegment", "year", "weekyear"]).agg( { "date": np.min,  
                                              "sales": np.sum,  "predict" :np.sum  }).reset_index()


 try :
    dfb_all= pd.concat(( dfb_all, dfb )) 
 except :
    dfb_all=  copy.deepcopy(dfb)    
    
 ii+=1   
 print ii

folder = "E:/Dropbox/pred_details/"
dfb_all[["machine_code", "productsegment",  "date", "sales", "predict"  ] ].to_csv(folder + '/output_pred_all_SKU.csv', mode='w')







#####################################################################################
#####################################################################################
# dfa= pd.read_csv( folder + "/" + input_file  ).fillna(0.0)

dfall= pd.read_csv( "E:/Dropbox/pred_details/vm_50_list_all.csv"  ).fillna(0.0)
ll1= list(dfall[[ "machine_code" ]].drop_duplicates().values)


date_start= 20160619

for  x in ll1 :
 dfa= copy.deepcopy(dfall[ (dfall.machine_code== x[0] )  ])


 ####Date Transformation   #####################################################
 dfa["month"]=    dfa["date"].apply( lambda x :  int(str(x)[4:6]) ).fillna(0).astype("int16")
 dfa["year"]=     dfa["date"].apply( lambda x :  int(str(x)[0:4]) ).fillna(0).astype("int16")

 dfa["weekyear"]= dfa["date"].apply( lambda x : weekyear_excel(x)  )
 dfa["weekday"]=  dfa["date"].apply( lambda x : weekday_excel(x)  )

 dfa["dayyear"]  = dfa.apply( lambda x : 7*(x["weekyear"]-1) + x["weekday"] , axis=1)




 ####Add intermerdiate results   ###############################################
 dfa["ma20"] = dfa["sales"].shift(1).rolling(window=20  ).mean().fillna(0)


 dfa["ma0"] = dfa["sales"] - dfa["ma20"]


 #### ddof=1 : N-1    or   ddof= 0  Divides by N
 dfa["ma0_stdev"] = dfa["ma0"].shift(1).rolling(window=12  ).std(ddof=0).fillna(0)

 dfa["ma1"]=   dfa["ma0"]  /  dfa["ma0_stdev"] 



 ### Prediction  ####################################################################
 ma1_past= dfa.groupby([ "year", "weekyear", "weekday"  ]).agg({"ma1": "mean"}).reset_index()
 ma1_past = ma1_past.replace([np.inf, -np.inf], 0.0)

 ma1_past["idx"]= ma1_past.apply(lambda x: x["year"]*10000 + x["weekyear"]*100 + x["weekday"], axis=1 ).astype("int")
 df= copy.deepcopy( ma1_past )

 df= df[df.idx > 20144301]




 dfa["ma1_predict"]= dfa.apply( lambda x : get_ma1(x["date"]), axis=1)

 dfa["predict"]= dfa["ma0_stdev"]  * dfa["ma1_predict"] + dfa["ma20"]



 dfb= copy.deepcopy(dfa[ dfa.date > date_start ])
 dfb= dfb.groupby([ "machine_code",  "year", "weekyear"]).agg( { "date": np.min,  
                                              "sales": np.sum,  "predict" :np.sum  }).reset_index()


 try :
    dfb_all= pd.concat(( dfb_all, dfb )) 
 except :
    dfb_all=  copy.deepcopy(dfb)    
 print(len(dfb_all))


folder = "E:/Dropbox/pred_details/"
dfb_all[["machine_code",   "date", "sales", "predict"  ] ].to_csv(folder + '/output_pred_all_.csv', mode='w')



dfall.productsegment.drop_duplicates()





import os

os.environ


os.system("cd D:\_devs\Python01\project27\github\configmy")



line= '__version__ = "0.12.2"'
lversion  =    line.split("=")[-1].replace('"', '').replace("'","").lstrip().split(".")
lversion[-1]=  str(int(lversion[-1]) + 1)
s1= '__version__ = ' + '"' +  '.'.join(lversion) + '"'






import numpy as np
import bcolz


a1= np.arange(0, 10**6)


c = bcolz.carray(a1, rootdir= os.getcwd() + '/obppc_vacs_storage.bcolz', mode='w', chunklen=2**4 )
c.flush()













 '''
 ### Future Predict
 for ii, x in dfa.iterrows() :
  if dfa.loc[ii, "sales"]== 0.0 : 
      break;
 iimax= ii


 ### Future Predict
 ii=0
 for ii,x in dfa.iterrows() :
  if ii > iimax-1 :
   
   ma1_predict= get_ma1(x["date"]) 
   predict=  dfa.loc[ii-1, "ma0_stdev"]  * ma1_predict + dfa.loc[ii-1, "ma20"] 
   dfa.loc[ii, "predict"]=      predict

   dfa.loc[ii, "ma20"]=         dfa.loc[ii-20:ii, "predict"].mean()  
   dfa.loc[ii, "ma0"] =         dfa.loc[ii, "predict"] - dfa.loc[ii, "ma20"]
   dfa.loc[ii, "ma0_stdev"]=    dfa.loc[ii-12:ii, "ma0"].std()  
   dfa.loc[ii, "ma1_predict"]=   ma1_predict
 '''
 






'''
dfa["error_diff"]=  dfa["predict"] - dfa["sales"]
dfa["error_cum"]=   dfa["error_diff"].shift(1).rolling(window=5  ).sum().fillna(0)


dfa["predict2"]= 0
dfa["error_diff2"]= 0
dfa["error_cum2"]= 0


err_cum=0.0
for ii,x in dfa.iterrows() :
 if ii > 6 :   
   predict2 = dfa.loc[ii, "predict"] -  0.0 *  err_cum
   dfa.loc[ii, "predict2"]= predict2
   dfa.loc[ii, "error_diff2"]= predict2 - dfa.loc[ii, "sales"]

   err_cum=  dfa.loc[ii-5:ii, "error_diff2"].sum()
   dfa.loc[ii, "error_cum2"]=  err_cum
'''   
   








