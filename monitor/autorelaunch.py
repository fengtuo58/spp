'''
##### Streaming for mapping key   #######################33####################
python  monitor/autorelaunch.py   --nsleep 30  --tempfile zprocess.txt
--tempfile zprocess.txt  

   Format of  zprocess.txt   :
   pid, folder, cmdTolaunch, time


TO DO :
   1) Create test python PID process writing on  zprocess.txt
   2) Add file locking to prevent concurrent writes
  

Programs asks to be monitored by writing on disk.
autorelaunch relaunches the program bby reading the file.


'''



################################################################################s
import os, sys, platform, arrow, pandas as pd,  copy, gc
import time, json, logging
from attrdict import AttrDict as dict2
import ast, re, executor
from time import sleep
import random
import subprocess  





###############################################################################

def os_getparent(dir0):

    return os.path.abspath(os.path.join(dir0, os.pardir))

    

try:    DIRCWD = os_getparent( os.path.dirname(os.path.abspath(__file__)) )

except:

    try:

        if sys.argv[0] == '': raise Exception

        DIRCWD = os_getparent( os.path.abspath(os.path.dirname(sys.argv[0])) )

    except:

        DIRCWD = '/mnt/hgfs/project27_raku/git_dev/agit_sortrank/'

        

        

###############################################################################

try:
    import argparse

    ppa = argparse.ArgumentParser()  # Command Line input
    ppa.add_argument('--DIRCWD',     type=str, default='',     help=' Root Folder')
    ppa.add_argument('--do',         type=str, default='zdoc', help='action'
    ppa.add_argument('--verbose',    type=int, default=0,      help=' Verbose mode')
    ppa.add_argument('--test',       type=int, default=0,      help=' test mode')

    ppa.add_argument('--tempfile',   type=str, default='zprocess.txt', help=' outputdata_dir')
    ppa.add_argument('--logfile',    type=str, default='zlog_kafka.txt', help=' outputdata_dir')    
    ppa.add_argument('--nsleep',     type=int, default=30,      help=' freq of log')   
    ppa.add_argument('--mode',       type=str, default='staging',      help=' staging/production kafka')           

    arg = ppa.parse_args()

    if arg.DIRCWD != '':  DIRCWD = arg.DIRCWD
s
except Exception as e:
    print('error into parsing arguments')
    print(e) ; sys.exit(1)

os.chdir(DIRCWD)  ; sys.path.append(DIRCWD + '/aapackage')





###############################################################################
APP_ID     =   __file__ + ',' + str(os.getpid()) + '_' +  str(random.randrange(10000))







if not os.path.exists( arg.tempfile ) : 
  print('Writing the file')  
  with open(arg.tempfile, mode='a')  as f1 :
     f1.write( ','.join( ['pid' , 'folder', 'bashcmd', 'time' ] ) + '\n' )     





def now():
   return  arrow.utcnow().to('Japan').format("YYYYMMDD_HHmmss")







def subprocess_cmd(command) :
    process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
    return process.pid

    # proc_stdout = process.communicate()[0].strip()
    # print proc_stdout



# subprocess_cmd('echo a; echo b')




while True :
  try :    s
    print('1. Reading the file', now())  
    df  = pd.read_csv( arg.tempfile   )
    df  = df.drop_duplicates( 'pid' )

  

    for i, row in df.iterrows() :

      pid_status = os.system( 'ps -p ' + str( row['pid'] ) )
      print( pid_status )
      if pid_status != 0  :    
        cmds      = 'cd ' +row['folder'] + ';  chmod -R 777 . ; ' + row['bashcmd']
        pid_start = subprocess_cmd( cmds )
        # pid_start = 5
        print('Relaunch PID :  ',  pid_start )
        df.loc[ i , 'pid' ] =  pid_start

  

    df.to_csv( arg.tempfile , index=False, mode='w')
    print('1. Writing the file',  now() )  
    sleep( arg.nsleep )

  except Exception as e :
    print( e )  
















    

    

    

    

    

    

    

    











