# coding: utf-8


import subprocess
import logging
import time
import csv
import psutil
from logging.config import dictConfig
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


Mb = 1024 * 1024
MAX_MEMORY = '1024'  # str is for an argparser type conversion
DELAY = 5
TERMINATE_TIMEOUT = 3
ST_ZOMBIE = 'zombie'
DEFAULT_LOG_LEVEL = 'debug'
LOG_FILENAME = 'monitor.log'
LOG_MAX_MB = '10'
LOG_BCOUNT = 3


def mb_type(string):
    count = int(string)
    return count * Mb


def log_level_type(string):
    level = string.upper()
    if not logging._nameToLevel.get(level):
        raise ValueError('invalid log level ' + level)
    return level


def launch(commands):
    logging.info('launch processes')
    processes = {}
    for cmd in commands:
        try:
            p = psutil.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            processes[p] = cmd
            logging.info('pid {}, command launched: {}'.format(p.pid, cmd))
        except:
            logging.exception('')
    return processes


def terminate(processes):
    logging.info('terminate processes')
    for p in processes:
        p.terminate()
    gone, alive = psutil.wait_procs(processes, timeout=TERMINATE_TIMEOUT)
    for p in alive:
        p.kill()
        logging.warning('still alive process {} is killed'.format(p.pid))


def extract_commands(csv_file, has_header=False):
    logging.info('extract commands from {}'.format(csv_file))
    with open(csv_file, 'r', newline='') as file:
        reader = csv.reader(file, skipinitialspace=True)
        if has_header:
            headers = next(reader)  # pass header
            logging.debug('has header {}'.format(headers))
        commands = [row for row in reader]

    logging.debug(commands)
    return commands


def main(csv_file, max_memory, checking_delay, has_header):
    commands = extract_commands(csv_file, has_header)
    processes = launch(commands)

    try:
        while processes:
            has_issue = []
            logging.info('check memory and zombie')
            for p in processes:
                rss = p.memory_info().rss
                logging.debug('pid {}, status {}, rss memory {}'.format(p.pid, p.status(), rss))
                if p.status() == ST_ZOMBIE:
                    has_issue.append(p)
                elif rss >= max_memory:
                    logging.error('memory exceeding {} pid {}'.format(rss, p.pid))
                    has_issue.append(p)

            if has_issue:
                terminate(has_issue)
                commands = [processes[p] for p in has_issue]
                for p in has_issue:
                    del processes[p]

                relaunched_procs = launch(commands)
                processes.update(relaunched_procs)

            time.sleep(checking_delay)
    except:
        logging.exception('undefined error')

    logging.info('monitoring is finished')











import os
import sys
import psutil
import time
from time import gmtime, strftime
from os.path import expanduser
from optparse import OptionParser

logfile = None
homeDir = ""
logPath = ""
imageName = ""
errorName = "WerFault.exe"
log_enabled = False

def checkResponsiveness():

    #Method 1: Check if process state is "Not Responding"
    os.system('tasklist /FI "IMAGENAME eq %s" /V /FO CSV > tmp.txt' % (imageName + ".exe"))

    tmp = open('tmp.txt', 'r')
    result = tmp.readlines()
    tmp.close()

    if len(result) > 1:
        if imageName in result[1] and "Not Responding" in result[1]:
            log("%s: Application %s Not-Responding. Verifying..." % (getCurrentTime(), imageName))
            time.sleep(15) #Wait 15 seconds and re-confirm that the app is not responding
            if imageName in result[1] and "Not Responding" in result[1]:
                return False
            else:
                log("%s: False alarm.\n" % (getCurrentTime(), imageName))
                return True

    #Method 2: If method 1 did not yield any results check if WerFault.exe is present and its window title matches our application name
    os.system('tasklist /FI "IMAGENAME eq WerFault.exe" /V /FO CSV > tmp.txt')

    tmp = open('tmp.txt', 'r')
    result = tmp.readlines()
    tmp.close()

    if len(result) > 1:
        if errorName in result[1] and imageName in result[1]:
            return False
        else:
            return True
    else:
        return True

def findProcess(name):
    procs = psutil.get_process_list()
    for proc in procs:
        try:
            if name in proc.name():
                return proc.pid
        except psutil.AccessDenied:
            continue
    return -1

def getCurrentTime():
    return strftime("%Y-%m-%d %H:%M:%S")

def log(message):
    print message
    if log_enabled: logfile.write(message + "\n")

def monitor():
    try:
        #Main loop. Polls for the application to start and waits until it crashes and then -> rinse and repeat.
        while 1:
            pid = -1
            log("%s: Polling for target application: %s to start..." % (getCurrentTime(), imageName))

            while pid == -1:
                pid = findProcess(imageName)
                if pid != -1:
                    break;
                time.sleep(5)

            log("%s: Target application %s found with PID: %d. Monitoring started..." % (getCurrentTime(), imageName, pid))

            #Get the process tied to this PID
            p = psutil.Process(pid)

            while p.is_running():
                #If checkResponsiveness returns false it means our target process is not responsive. Kill it with Fire!
                if checkResponsiveness() is False:
                    log("%s: %s Seems to be hanging. Attempting to kill it!" % (getCurrentTime(), imageName))
                    time.sleep(1)
                    p.kill()

                    #Search and Destroy any WerFault processes still lingering
                    pid = -1
                    while 1:
                        pid = findProcess(errorName)
                        if pid != -1:
                            log("%s: Killing WerFault process with PID %d" % (getCurrentTime(), pid))
                            p = psutil.Process(pid)
                            p.kill()
                        else:
                            break

                time.sleep(5)    #Sleep before polling again

    except KeyboardInterrupt:
        print "Keyboard interrupt received. Closing..."
        if log_enabled: logfile.close()

if __name__ == "__main__":
    parser = OptionParser(usage="Usage: %prog [OPTIONS]\nTry '%prog --help' for more information.", version="%prog 1.0")
    parser.add_option("-n", "--name",
                  action="store",
                  dest="imagename",
                  default="",
                  help="target application to monitor",)
    parser.add_option("-l", "--log",
                  action="store_true",
                  dest="log_flag",
                  default=False,
                  help="enable logging",)
    (options, args) = parser.parse_args()

    imageName   = options.imagename.strip(".exe")
    log_enabled = options.log_flag

    if imageName == "":
        print "Please specify a valid application name! Use python '%s --help' for more information." % (sys.argv[0])
        sys.exit(0)

    if log_enabled:
        homeDir = expanduser("~")                        #Get users homedir for logging purposes.
        logPath = homeDir + "\\Process Monitor Logs\\"   #Set log directory.

        if not os.path.exists(logPath):
            os.mkdir(logPath)

        filename = "%s.log" % (strftime("%Y-%m-%d_%H-%M-%S"))
        logfile = open("%s%s" % (logPath, filename), "w")
        print "%s: Logging enabled: %s%s" % (getCurrentTime(), logPath, filename)

    monitor()   #Start monitoring for crashes
