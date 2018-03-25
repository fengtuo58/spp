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
            logging.info('check a work completion')
            completed = []
            for p in processes:
                logging.debug('pid {}, status {}'.format(p.pid, p.status()))
                if p.status() == ST_ZOMBIE:
                    logging.info('work of pid {} is completed'.format(p.pid))
                    completed.append(p)
            for p in completed:
                del processes[p]

            exceeded = []
            logging.info('check memory')
            for p in processes:
                rss = p.memory_info().rss
                logging.debug('pid {}, rss memory {}'.format(p.pid, rss))
                if rss >= max_memory:
                    logging.error('memory exceeding {} pid {}'.format(rss, p.pid))
                    exceeded.append(p)

            if exceeded:
                terminate(exceeded)
                commands = [processes[p] for p in exceeded]
                for p in exceeded:
                    del processes[p]

                relaunched_procs = launch(commands)
                processes.update(relaunched_procs)

            time.sleep(checking_delay)
    except:
        logging.exception('undefined error')

    logging.info('monitoring is finished')


if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('csv_file', help='either an absolute path or a file name is from a working directory')
    parser.add_argument('--has-header', action='store_true', help='just type this option if csv file has a header row')
    parser.add_argument('--max-memory', type=mb_type, default=MAX_MEMORY,
                        help='unit is Mb')
    parser.add_argument('--checking-delay', type=int, default=DELAY,
                        help='in what time processes will be checked for a memory consumption in seconds')
    parser.add_argument('--log-level', type=log_level_type, default=DEFAULT_LOG_LEVEL,
                        help='level string of the python logging system')
    parser.add_argument('--log-filename', default=LOG_FILENAME,
                        help='either an absolute path or a file name is from a working directory')
    parser.add_argument('--log-maxmb', type=mb_type, default=LOG_MAX_MB,
                        help='unit is Mb')
    parser.add_argument('--log-bcount', type=int, default=LOG_BCOUNT,
                        help='log backup count')
    args = parser.parse_args()

    log_config = {'version': 1,
                  'formatters': {
                      'common': {
                          'class': 'logging.Formatter',
                          'format': '[%(asctime)s %(levelname)s] %(message)s'
                      },
                  },
                  'handlers': {
                      'common': {
                          'class': 'logging.handlers.RotatingFileHandler',
                          'formatter': 'common',
                          'level': args.log_level,
                          'filename': args.log_filename,
                          'maxBytes': args.log_maxmb,
                          'backupCount': args.log_bcount,
                      },
                  },
                  'loggers': {
                      '': {
                          'handlers': ['common'],
                          'level': args.log_level,
                      },
                  }
              }
    dictConfig(log_config)

    main(args.csv_file, args.max_memory, args.checking_delay, args.has_header)
