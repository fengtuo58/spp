# -*- coding: utf-8 -*-

'''

python pymonitor_cli.py  --name couchbase_update  --max_memory 120  --nprocess 2   --consumergroup gr100  --nlogfreq 5000     


'''

if __name__ != '__main__' : sys.exit(0)



import os, sys, platform, arrow, pandas as pd, numpy as np,  copy, gc
import time, json

#from couchbase.cluster import Cluster
#from couchbase.cluster import PasswordAuthenticator

from attrdict import AttrDict as dict2
import ast, re, random, psutil
from executor import execute
from time import sleep
import time, csv, subprocess
import shlex



###############################################################################
###############################################################################

def os_getparent(dir0):
    return os.path.abspath(os.path.join(dir0, os.pardir))

    

try:  
      DIRCWD = subprocess.check_output( 'git rev-parse --show-toplevel'.split(' ') ).rstrip().decode('utf-8') 

except:
    try:
        DIRCWD = os_getparent( os.path.dirname(os.path.abspath(__file__)) )
        if sys.argv[0] == '': raise Exception
        DIRCWD = os_getparent( os.path.abspath(os.path.dirname(sys.argv[0])) )

    except:
        DIRCWD = '/mnt/hgfs/project27_raku/git_dev/agit_sortrank/'

        

        



###############################################################################

try:

    import argparse
    ppa = argparse.ArgumentParser()  # Command Line input
    ppa.add_argument('--DIRCWD',     type=str, default='',     help=' Root Folder')
    ppa.add_argument('--do',         type=str, default='zdoc', help='action')
    ppa.add_argument('--verbose',    type=int, default=0,      help=' Verbose mode')
    ppa.add_argument('--test',       type=int, default=0,      help=' test mode')


    ppa.add_argument('--nprocess',   type=int, default=1,      help=' test mode')       
    ppa.add_argument('--name',       type=str, default='',      help=' test mode')        
    ppa.add_argument('--max_memory',   type=int, default=120,      help=' test mode')     
    ppa.add_argument('--logfile', type=str, default='zlog_kafka.txt', help=' outputdata_dir')        



    ppa.add_argument('--consumergroup',       type=str, default='group1',      help=' test mode')       
    ppa.add_argument('--configfile', type=str, default='/config/config.txt', help=' outputdata_dir')
    ppa.add_argument('--nlogfreq',       type=int, default=10000,      help=' test mode')    
    arg = ppa.parse_args()

    if arg.DIRCWD != '':  DIRCWD = arg.DIRCWD



except Exception as e:
    print('error into parsing arguments')
    print(e) ; sys.exit(1)

os.chdir(DIRCWD)  ; sys.path.append(DIRCWD + '/aapackage')
print( DIRCWD )





###############################################################################
###############################################################################
APP_ID =  __file__ + ',' + str(os.getpid()) + '_' +  str(random.randrange(10000))

global cmds


cmds = [

      'python kafkastreaming/streaming_couchbase_update_cli.py    --verbose 1  --test 1  --nsleep 1  --logfile /data_share/search_data/zlog_kafka1.txt      '

     ,'python kafkastreaming/streaming_couchbase_update_cli.py    --verbose 1  --test 1  --nsleep 1  --logfile /data_share/search_data/zlog_kafka2.txt      '   

    ]





params = {
'max_memory' ; arg.max_memory        


}









###############################################################################

def printlog(s='', s1='', s2='', s3='', s4='', s5='', s6='', s7=''):

  prefix = APP_ID + ',' + arrow.utcnow().to('Japan').format("YYYYMMDD_HHmmss,") +','



  s = ','.join( [ prefix, str(s), str(s1), str(s2), str(s3), str(s4), str(s5) , str(s6), str(s7), '\n' ] )

  print(s) 

  with open( arg.logfile, mode='a') as f1 :

    f1.write( s )

printlog( ' start'  )

    







###############################################################################

###############################################################################

Mb = 1024 * 1024

DELAY = 5

TERMINATE_TIMEOUT = 3





def mb_type(string):

    count = int(string)

    return count * Mb





def launch(commands):

    processes = []

    for cmd in commands:

        try:

            p =subprocess.Popen( cmd, shell= False  )

            processes.append( p.pid )

            print('Launching: ', p.pid,  cmd )

        except Exception as e :

            print( e )

    return processes





def terminate(processes):

    for p in processes:

      try :

         p.kill()

         print('killed ', p.pid )

      except Exception as e :

         pass





def extract_commands(csv_file, has_header=False):

    with open(csv_file, 'r', newline='') as file:

        reader = csv.reader(file, skipinitialspace=True)

        if has_header:

            headers = next(reader)  # pass header

        commands = [row for row in reader]



    return commands





def monitor(pars, nfreq):

    # commands  = extract_commands(csv_file, has_header)

    cmds2 = []

    for cmd in cmds :

      ss = shlex.split(cmd)

      cmds2.append( ss )



    processes = launch( cmds2 )

    try:

      while True :

        has_issue = []

        ok_process = []

        for pidi in processes:

          p = psutil.Process( pidi )

          pdict = p.as_dict()

            



          if not psutil.pid_exists(p.pid) : 

             has_issue.append(p)

             print('Process has been killed ', p.pid)



          elif pdict['.status'] == 'zombie' :  

             has_issue.append(p)

             print('Process Over zombie ', p.pid)



          elif pdict['memory_full_info'][0] >= pars['max_memory'] :        

             has_issue.append(p)

             print('Process Over max memory ', p.pid)

          

            

          ##  Add new check  

            

            

            

            

          else :

             ok_process.append( p.pid ) 

              



        for p in has_issue :

          pcmdline = p.cmdline()  

          pidlist= launch( [ pcmdline ] )   # New process can start before

          terminate( [ p ] )  



          if len( pidlist  ) > 0 :

            ok_process.append( pidlist[0] )

        

        processes = copy.deepcopy( ok_process )          

        print('Waiting....')

        time.sleep(nfreq)

        

    except Exception as e :

         print(e)









#########################################################################

#########################################################################

monitor( params , 20 )



















'''



cmd1 = '  python   kafkastreaming/streaming_test1.py    --consumergroup group10  --nlogfreq 500    --logfile zlog_kafka_prod2.txt   '

cmd1 = cmd1.split()

p = subprocess.Popen( cmd1 , shell=False )

print(p.pid, p)

#print( p.stdout.read() )





sys.exit(0)

###############################################################################

###############################################################################

import psutil





while True :

 for pidi in pid_list :    

   p = psutil.Process( pidi )

   if p.status == psutil.STATUS_ZOMBIE :

     print('zombie')









p = psutil.Process(  6562 )





pdict = p.as_dict()







MBytes = 1.0 * 10**6

p.memory_full_info().rss  /MBytes

























###############################################################################

###############################################################################

p = psutil.Popen(['python' , '/mnt/hgfs/project27_raku/git_dev/git_staging/dsd/devstreaming/kafkastreaming/streaming_couchbase_update_cli.py '

 ] ,    stdout=subprocess.PIPE )

print(p)

print( p.stdout.read() )





p = psutil.Popen(['pwd' ] , 

                 stdout=subprocess.PIPE )





print(p)

print( p.stdout.read() )







sys.exit()







'''







'''





>>>

>>> p.pid

7055

>>> p.ppid()

7054

>>> p.parent()

<psutil.Process(pid=7054, name='bash') at 140008329539408>

>>> p.children()

[<psutil.Process(pid=8031, name='python') at 14020832451977>,

 <psutil.Process(pid=8044, name='python') at 19229444921932>]

>>>

>>> p.status()

'running'

>>> p.username()

'giampaolo'

>>> p.create_time()

1267551141.5019531

>>> p.terminal()

'/dev/pts/0'

>>>

>>> p.uids()

puids(real=1000, effective=1000, saved=1000)

>>> p.gids()

pgids(real=1000, effective=1000, saved=1000)

>>>

>>> p.cpu_times()

pcputimes(user=1.02, system=0.31, children_user=0.32, children_system=0.1)

>>> p.cpu_percent(interval=1.0)

12.1

>>> p.cpu_affinity()

[0, 1, 2, 3]

>>> p.cpu_affinity([0, 1])  # set

>>> p.cpu_num()

1

>>>

>>> p.memory_info()

pmem(rss=10915840, vms=67608576, shared=3313664, text=2310144, lib=0, data=7262208, dirty=0)

>>> p.memory_full_info()  # "real" USS memory usage (Linux, OSX, Win only)

pfullmem(rss=10199040, vms=52133888, shared=3887104, text=2867200, lib=0, data=5967872, dirty=0, uss=6545408, pss=6872064, swap=0)

>>> p.memory_percent()

0.7823

>>> p.memory_maps()

[pmmap_grouped(path='/lib/x8664-linux-gnu/libutil-2.15.so', rss=32768, size=2125824, pss=32768, shared_clean=0, shared_dirty=0, private_clean=20480, private_dirty=12288, referenced=32768, anonymous=12288, swap=0),

 pmmap_grouped(path='/lib/x8664-linux-gnu/libc-2.15.so', rss=3821568, size=3842048, pss=3821568, shared_clean=0, shared_dirty=0, private_clean=0, private_dirty=3821568, referenced=3575808, anonymous=3821568, swap=0),

 pmmap_grouped(path='/lib/x8664-linux-gnu/libcrypto.so.0.1', rss=34124, rss=32768, size=2134016, pss=15360, shared_clean=24576, shared_dirty=0, private_clean=0, private_dirty=8192, referenced=24576, anonymous=8192, swap=0),

 pmmap_grouped(path='[heap]',  rss=32768, size=139264, pss=32768, shared_clean=0, shared_dirty=0, private_clean=0, private_dirty=32768, referenced=32768, anonymous=32768, swap=0),

 pmmap_grouped(path='[stack]', rss=2465792, size=2494464, pss=2465792, shared_clean=0, shared_dirty=0, private_clean=0, private_dirty=2465792, referenced=2277376, anonymous=2465792, swap=0),

 ...]

>>>

>>> p.io_counters()

pio(read_count=478001, write_count=59371, read_bytes=700416, write_bytes=69632, read_chars=456232, write_chars=517543)

>>>

>>> p.open_files()

[popenfile(path='/home/giampaolo/svn/psutil/setup.py', fd=3, position=0, mode='r', flags=32768),

 popenfile(path='/var/log/monitd', fd=4, position=235542, mode='a', flags=33793)]

>>>

>>> p.connections()

[pconn(fd=115, family=<AddressFamily.AF_INET: 2>, type=<SocketType.SOCK_STREAM: 1>, laddr=addr(ip='10.0.0.1', port=48776), raddr=addr(ip='93.186.135.91', port=80), status='ESTABLISHED'),

 pconn(fd=117, family=<AddressFamily.AF_INET: 2>, type=<SocketType.SOCK_STREAM: 1>, laddr=addr(ip='10.0.0.1', port=43761), raddr=addr(ip='72.14.234.100', port=80), status='CLOSING'),

 pconn(fd=119, family=<AddressFamily.AF_INET: 2>, type=<SocketType.SOCK_STREAM: 1>, laddr=addr(ip='10.0.0.1', port=60759), raddr=addr(ip='72.14.234.104', port=80), status='ESTABLISHED'),

 pconn(fd=123, family=<AddressFamily.AF_INET: 2>, type=<SocketType.SOCK_STREAM: 1>, laddr=addr(ip='10.0.0.1', port=51314), raddr=addr(ip='72.14.234.83', port=443), status='SYN_SENT')]

>>>

>>> p.num_threads()

4

>>> p.num_fds()

8

>>> p.threads()

[pthread(id=5234, user_time=22.5, system_time=9.2891),

 pthread(id=5235, user_time=0.0, system_time=0.0),

 pthread(id=5236, user_time=0.0, system_time=0.0),

 pthread(id=5237, user_time=0.0707, system_time=1.1)]

>>>

>>> p.num_ctx_switches()

pctxsw(voluntary=78, involuntary=19)

>>>

>>> p.nice()

0

>>> p.nice(10)  # set

>>>

>>> p.ionice(psutil.IOPRIO_CLASS_IDLE)  # IO priority (Win and Linux only)

>>> p.ionice()

pionice(ioclass=<IOPriority.IOPRIO_CLASS_IDLE: 3>, value=0)

>>>

>>> p.rlimit(psutil.RLIMIT_NOFILE, (5, 5))  # set resource limits (Linux only)

>>> p.rlimit(psutil.RLIMIT_NOFILE)

(5, 5)

>>>

>>> p.environ()

{'LC_PAPER': 'it_IT.UTF-8', 'SHELL': '/bin/bash', 'GREP_OPTIONS': '--color=auto',

'XDG_CONFIG_DIRS': '/etc/xdg/xdg-ubuntu:/usr/share/upstart/xdg:/etc/xdg', 'COLORTERM': 'gnome-terminal',

 ...}

>>>

>>> p.as_dict()

{'status': 'running', 'num_ctx_switches': pctxsw(voluntary=63, involuntary=1), 'pid': 5457, ...}

>>> p.is_running()

True

>>> p.suspend()

>>> p.resume()

>>>

>>> p.terminate()

>>> p.wait(timeout=3)



'''



















