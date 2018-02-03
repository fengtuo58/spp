#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
1/ Look ito todo/
2/ Analyze task_*.py header (text parsing) --->  Calculate Nb of Input Split  (numpy split).
3/ Split the input data  (dataframe, csv) --> into several files ( use pickle to save )
4/ Launch in paralell task_0002.py with different input  ( using  file_slit_XXXX) using sub_process
5/When script is finished ---> Move script into  with name:  task_0001_20160202HHMMSS.py
6/ Write in log_file
----------------------------------------------------------------------------------------------------
Input :
  folder_root :   /myfolderoftasks/
  start_time  :   2017-12-03 15:45
  timeout_time :  2025-12-03 15:45

Folder Structure:
  /folder_root/  task1_finished/      #finished
                                task1.py
                                file2.pkl
                                otherfile
                                /tmpfolder/
                                /output/

                 task2_failed/    #failed to launch
                                task1.py
                                file2.pkl
                                /tmpfolder/
                                /output/

                 tasks3/    #Not yet launched
                        task1.py
                        inputfile1.pkl


                 logfile.txt

task_launcher.py
  import task_parallel as taskpa
  taskpa.execute("/myfolder/", starttime="2017-03-05 11:45")


python   util_parallel.py --do test


'''
import os, sys, platform, pandas as pd,  subprocess, re, ast

def os_getparent(dir0):
    return os.path.abspath(os.path.join(dir0, os.pardir))

try:    DIRCWD = os_getparent(os.path.dirname(os.path.abspath(__file__)))
except:
    try:
        if sys.argv[0] == '': raise Exception
        DIRCWD = os_getparent(os.path.abspath(os.path.dirname(sys.argv[0])))
    except:
        DIRCWD = '/mnt/hgfs/project27_raku/git_dev/agit_sortrank/'

try:
    import argparse
    ppa = argparse.ArgumentParser()  # Command Line input
    ppa.add_argument('--DIRCWD', type=str, default='', help=' Root Folder')
    ppa.add_argument('--do', type=str, default='user', help=' user/item/rating')

    arg = ppa.parse_args()
    if arg.DIRCWD != ''  :  DIRCWD = arg.DIRCWD

except Exception as e:
    print(e);
    sys.exit(1)
os.chdir(DIRCWD); sys.path.append(DIRCWD + '/aapackage')

##################################################################################################
import subprocess, shlex, datetime, time, pickle,  arrow, psutil




#-------------------------------Util functions ####################################################
# code to save and load the splited data
def z_key_splitinto_dir_name(keyname) :
    lkey= keyname.split
    if len(lkey)== 1 :dir1=""
    else : dir1= '/'.join(lkey[:-1]); keyname= lkey[-1]
    return dir1, keyname


def py_save_object(obj, folder='/folder1/keyname', isabsolutpath=0):
    if isabsolutpath==0 and folder.find('.pkl') == -1 : #Local Path
        dir0, keyname= z_key_splitinto_dir_name(folder)
        os.makedirs(DIRCWD+'/aaserialize/' + dir0)
        dir1= DIRCWD+'/aaserialize/' + dir0 + '/'+ keyname + '.pkl'
    else :
        dir1= folder
    with open( dir1, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        return dir1


def py_load_object(folder='/folder1/keyname', isabsolutpath=0, encoding1='utf-8'):
    '''def load_obj(name, encoding1='utf-8' ):
        with open('D:/_devs/Python01/aaserialize/' + name + '.pkl', 'rb') as f:
        return pickle.load(f, encoding=encoding1)'''
    if isabsolutpath==0 and folder.find('.pkl') == -1 :
        dir0, keyname= z_key_splitinto_dir_name(folder)
        os.makedirs(DIRCWD+'/aaserialize/' + dir0)
        dir1= DIRCWD+'/aaserialize/' + dir0 + '/'+ keyname + '.pkl'
    else :
        dir1= folder
    with open(dir1, 'rb') as f:
        return pickle.load(f)



#---------------------------------------------------------------------------------------------------------------
class task_parallel():
    '''This is the main class of this script
        IT controls the basic flow of the program
        and defines all of the global variables'''

    def __init__(self, folder_root, timeout=0, verbose=False):
        ''' This function has all of the variables, you have to provide it the folder architecture, the timeout
            and the DIRCWD variable.'''
        ##### all of the string variables #####
        self.folder_root = folder_root
        self.stdout = ""                                    # this is a dummy variable for the stdout of the running script
        self.stderr = ""                                    # this is the same but for stderr

        ##### all of the file variables #####
        self.log_fp = open(folder_root+'/logfile.txt', "w")

        #### all of the arrays #####
        self.task_names = []

        ##### all of the dictionaries #####
        self.header_vars = dict()                           # dictionnary of variables found in the header
        self.env_vars = dict()                              # python envornement variable   ! this var is not used but has to be here
        self.task_list = dict()                             # task_list['task_name'] = [ status, [task_executables], [input_files], start_time, pid ]
                                                            # status: 0, 1, 2 | to_run, failed, finished // declared in self.retrieve_folders
        ##### all of the time variables #####
        self.timeout =    arrow.get(timeout)
        self.start_time = arrow.now()

        ##### all of the other variables #####
        self.DIRCWD = DIRCWD
        self.sub_proc = None
        self.v = verbose

    #---------------------misc fucntions----------------------------------------
    def run_check_timeout(self):
        if arrow.now() > self.timeout:  return True
        else:    return False

    def script_error_exit(self, msg, return_code=-1):
        ''' print_error function print to stderr the print_error and its line for easy correction '''
        sys.stderr.write(msg)
        sys.exit(return_code)

    def log_write(self, string, time=arrow.now().format('YYYY-MM-DD HH:mm:ss')):
        ''' function to log the given string to the logfile.
        '''
        self.log_fp.write('[%s] %s\n' % (time, string))
        self.log_fp.flush()
        if self.v:
            print(string)

    #---------------------header functions--------------------------------------
    def script_header_isok(self, fname):
        ''' chek if header has the correct variables such as,
            NSPLIT | ARRAY '''
        try:
            if 'NSPLIT' in self.header_vars[fname] and 'ARRAY' in self.header_vars[fname]: return True
            else: return False
        except: # if the index is out of range, it means that the scripts had an EOF error so the header is not ok/ not existant
            return False

    def script_header_scan(self, fname):
        ''' Scan's throught the header, get all of the important variables inside of a dictionary,
            and then run the code that is inside of the header. Note, watch out for the EOF print_error
            inside of the header if python recognises an EOF inside of that header it will be broken. '''
        in_header = False
        source = ""

        self.log_write("Scanning header for %s" % fname)
        with open(fname, "r") as f:                             # open the file
            for line in f.readlines():
                if "START_HEADER_SCRIPT" in line:               # figure out if you are at the start of the
                    in_header = True                            # header or not, set a variable true if it is
                else:
                    if in_header:
                        if "END_HEADER_SCRIPT" in line:         # if found this string you are no more in the header so you will leave
                            break                               # it.
                        else:                                   # it has to retrieve the source code to execute
                            source += line                      # adding source to a var
        self.header_vars[fname] = dict()
        try:
            exec(source, self.env_vars, self.header_vars[fname])   # executing source, watch out for the EOF print_error,
        except EOFError:                                           # if python find EOF in source it will crash
            self.log_write("EOF print_error in the script: {0}".format(fname))
            self.log_write("please check the data in the source header")

    def run_headers(self):
        ''' run the individual headers and get all of the data that are in the headers
        '''
        if len(self.task_names) == 0:
            self.log_write("No scripts to work with...")
            return
        for fd_name in self.task_names:
            for scripts in self.task_list[fd_name][1]:
                self.script_header_scan(self.folder_root + fd_name + '/' + scripts)
                if not self.script_header_isok(self.folder_root+fd_name+ '/'+scripts):
                    self.log_write('ERROR: script %s does not have a good header so is beeing ignored' % scripts)
                    del self.task_list[fd_name][1][self.task_list[fd_name][1].index(scripts)]
                    # I though t you had to remove timeout and pid but they are added in runcommand so the above line is good
                else:
                    self.log_write('script %s has been processed, and the header has been executed' % scripts)



    #--------------------folder functions----------------------------------------------------
    def folder_create_structure(self, name, task):
        ''' setup the structure of the given folder '''
        if os.path.isdir(name):
            self.log_write('created the folder structure for: %s' % name)
            if not os.path.exists(name+'/tmpfolder/'):
                self.log_write('creating tmp folder')
                os.makedirs(name + '/tmpfolder/')
            if not os.path.exists(name+'/output/'):
                self.log_write('creating output folder')
                os.makedirs(name + '/output/')
            if not os.path.exists(name+'/input/'):
                self.log_write('creating input folder')
                os.makedirs(name + '/input/')
            for i in os.listdir(name):
                if '.py' in i and i not in self.task_list[task][1]:
                    self.task_list[task][1].append(i)
                    self.log_write('found script: %s' % i)
                    with open(name+'/output/%s_stdout.txt' % i, 'w') as f:
                        f.close()
                    with open(name+'/output/%s_stderr.txt' % i, 'w') as f:
                        f.close()
                elif '.plk' in i:
                    self.task_list[task][2].append(i)
                    self.log_write('found input file: %s' % i)
        else:
            self.log_write('%s name is not a directory, please put scripts inside of a directory')



    def folder_retrieve_task(self):
        ''' this function checks if the folders exists and add the file in the todo array, it also
            creates, the individual task folders for the inputs and outputs of each individual tasks.
            it also sets and id inside of a map so that when you provide the name of the file you get its id. '''
        if not os.path.exists(self.folder_root):    self.script_error_exit('folder root does not exist')

        for folder in os.listdir(self.folder_root):
            if 'task' in folder and os.path.isdir(self.folder_root+folder):
                self.log_write('found task folder: %s' % folder)
                self.task_names.append(folder)
                self.task_list[folder] = [ None, [], [], dict(), dict()]   # see above for the detail of each element

                if 'finished' in folder:   self.task_list[folder][0] = 2             # finished folders those will get ignored
                elif 'failed' in folder:   self.task_list[folder][0] = 1             # failed folders those will get reexecuted to see
                else:                 self.task_list[folder][0] = 0                  # normal folders waiting to be executed
                self.folder_create_structure(self.folder_root + folder, folder)

        if len(self.task_list) == 0:  self.log_write("Did not find any task to work with...")


    def task_move_finished(self, script_name):
        ''' Function to rename the folders that finished being processed.
        '''
        folder_name = self.folder_root+script_name
        if script_name.rfind('/') != -1:   new_folder_name = self.folder_root+script_name[:script_name.rfind('/')]+'_finished/'
        else:                              new_folder_name = self.folder_root+script_name+'_finished/'

        if self.task_list[script_name][0] == 2 and 'finished' not in script_name:
            self.log_write("renamed %s to %s" % (folder_name, new_folder_name))
            os.rename(folder_name, new_folder_name)


    def task_move_failed(self, task):
        ''' function to muve failed tasks '''
        folder_name = self.folder_root + task
        if task.rfind('/') != -1: renamed_folder_name = self.folder_root + task[:task.rfind('/')]+'_failed'
        else:                     renamed_folder_name = self.folder_root + task + '_failed'

        if self.task_list[task][0] == 1 and 'failed' not in task:
            self.log_write('%s has failed so is beeing renamed to %s' % (task, renamed_folder_name))
            os.rename(folder_name, renamed_folder_name)


    #-------------------running scripts----------------------------------------------------------
    def run_command(self, command):
        ''' Function ot run a script, provide it a script formated like this "/script/path/name args" it will then
            cut this into an array for the Popen function, and it will also start the start_process_time for the
            individual process '''
        try:
            num = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            self.log_write("[*] {0}'s pid is : {1}".format(command, num.pid))
            return num                                      # return to manage it's output

        except:
            self.log_write("[*] {0} failed".format(command))
            return None                                     # return none so that the output does not get monitored


    def run_all_scripts(self):
        ''' This will run every script of every task asynchronously '''
        if len(self.task_names) == 0:  self.log_write("No task, with scripts to execute....")

        for task in self.task_names:
            for script in self.task_list[task][1]:
                self.log_write("running script: %s" % (self.folder_root+task+'/'+script))
                err_check = self.run_command(self.folder_root+task+'/'+script)
                time.sleep(3)

                if err_check is not None:
                    self.task_list[task][4][script] = err_check
                    self.task_list[task][3][script] = arrow.now()
                else:
                    #TODO: DELETE THE FAILED SCRIPT
                    self.task_list[task][4].append(None)
                    self.task_list[task][3].append(None)


    def script_is_running(self, task, script):
        ''' function to check if a script is running or not '''
        script_pid = self.task_list[task][4][script].pid
        if psutil.pid_exists(script_pid): return True
        else:                             return False


    #------------------process data files--------------------------------------------------------
    def data_split(self, NSPLIT, ar, folder_name):
        ''' splitting function to split the given arrays into a number nsplit of parts
            it gets the split size then saves it into sub arrays that then get save to the acording folders '''
        splitsize= int( len(ar) / NSPLIT )                  # Needs to be round number because files can not be
        output_varname= 'ar' + '_'                          # floats its 1 file not 1.5 file
        for ii in range(0, NSPLIT) :
            ar_i= ar[ii*splitsize:((ii+1)*(splitsize))] # split the different arrays in sub arrays and save them
            py_save_object(ar_i, folder_name + '/input/' + output_varname + str(ii), 1)

    def data_split_all(self):
        '''  Split all of the data, it lists throught the files in TODO_FILE, scans there header executes it,
            Checks the header and split the data found in the header '''
        if len(self.task_names) == 0:
            self.log_write("No data inside of headers...")
        for iname in self.task_names:
            for sc in self.task_list[iname][1]:
                self.log_write("spliting header data for %s" % sc)
                arr=     self.header_vars[self.folder_root+iname+'/'+sc]['ARRAY']          # set the array
                nsplit=  self.header_vars[self.folder_root+iname+'/'+sc]['NSPLIT']         # set the nsplit
                self.data_split(nsplit, arr, self.folder_root + iname)


    #-----------------monitoring----------------------------------------------------------------
    # TODO:dont forget to monitor timeout
    def task_isok(self, task):
        ''' check if the pid names and startime are all present if one is missing there is an error and the task
        will be removed '''
        if len(self.task_list[task][1]) == len(self.task_list[task][3]) and\
        len(self.task_list[task][1]) == len(self.task_list[task][4]) and\
        len(self.task_list[task][3]) == len(self.task_list[task][4]):   return True
        else:                                                           return False


    def task_isfinished(self, task):
        if len(self.task_list[task][1]) == 0:
            self.log_write("task %s is finished" % task)
            self.task_list[task][0] = 2
            self.task_move_finished(task)


    def task_all_running(self):
        num = 0
        for task in self.task_names:
            if self.task_list[task][0] == 2:
                self.log_write("task %s is finished" % task)
                num+=1
        if num == len(self.task_names):
            self.log_write("All task are finished!")
            return False
        else:
            return True


    def monitor_task(self, task):
        ''' function that monitors a task '''
        if not self.task_isok(task=task):
            del self.task_list[task]
            del self.task_names[task]
            self.log_write("task: %s is not complete there is a problem in the number of scripts / startime / pid" % task)
            return 1
        for script in self.task_list[task][1]:
            if self.script_is_running(task, script):
                err = self.monitor_script(script=script, task=task,\
                        pid=self.task_list[task][4][script])
            else:
                err = -1
            if err == -1:
                self.log_write("script : %s finished" % script)
                del self.task_list[task][1][self.task_list[task][1].index(script)]     # script name
                del self.task_list[task][4][script]     # process object
                del self.task_list[task][3][script]     # start time


    def monitor_script(self, script, task, pid):
        ''' function that monitors a single script '''
        stdout_fname = self.folder_root+task + '/output/%s_stdout.txt' % script
        stderr_fname = self.folder_root+task + '/output/%s_stderr.txt' % script
        stdout_fp = open(stdout_fname, 'a')
        stderr_fp = open(stderr_fname, 'a')
        out_data = pid.stdout.readline()
        err_data = pid.stderr.readline()

        if out_data == '' and err_data == '' and pid.poll() is not None:  return -1
        if out_data:
            self.log_write("got stdout data from %s" % script); stdout_fp.write(out_data)
        if err_data:
            try :
              self.log_write("got stderr data from %s" % script) ;   stderr_fp.write(err_data)
            except : pass

        stdout_fp.close();  stderr_fp.close()


    def monitor_all_tasks(self):
        ''' function to monitor all tasks and check if they have failed or timedout '''
        if len(self.task_names) == 0:
            self.log_write("No task to monitor...")
        while self.task_all_running() and not self.run_check_timeout():
            for task in self.task_names:
                self.monitor_task(task=task)
                self.task_isfinished(task)
        if self.run_check_timeout():
            for task in self.task_names:
                if self.task_list[task][0] != 2:
                    self.task_list[task][0] = 1
                    self.task_move_failed(task)
            self.log_write("Script Exection Timed Out!")




    #-----------------main function---------------------------------------------------------
    def run_main(self):
        ''' Main function that check all of the folders splits the data,
            then goes through the script to execute then executes then with task_run_scripts,
            it then waits for them to finish. '''
        self.log_write("--Starting up--")
        self.folder_retrieve_task()                     # setup the folder structure and retrieve all of the tasks
        self.run_headers()                           # run the scripts that where found and get the data from the headers
        # self.split_data_all()                       # split the data founs in the headers and save them as input
        self.run_all_scripts()                      # run each individual scripts and save usefull data about them
        self.log_write("--going into monitor loop--")
        self.monitor_all_tasks()



def execute(folder_root, starttime="2010-03-01 11:25", timeout="2020-01-01 11:00", verbose=1):
    ''' Launch a independant sub-process where the python code below is executed at time 2017 03 01 11:25
        this is a function wrapper for execute_sub
    '''

    starttime = arrow.get(starttime)
    timeout   = arrow.get(timeout)

    tpar = task_parallel(timeout=timeout, folder_root=folder_root, verbose=True)

    while arrow.now() < starttime :
         time.sleep(30)

    #### Execute in separate sub-process
    tpar.run_main()





# Test
if __name__ == "__main__" and  arg.do == 'test' :
    # execute('testing/', starttime=arrow.now().format('YYYY-MM-DD HH:mm:ss'), timeout='2000-01-01 00:00:00')
    #m = manage(timeout="2020-01-01 11:00" , folder_root='testing/', verbose=True)
    import util_parallel as tpar

    #todo_folder= "c:/My_todoFolder/"
    todo_folder="testing/"
    tpar.execute(todo_folder, starttime="2017-01-02-15:25")

