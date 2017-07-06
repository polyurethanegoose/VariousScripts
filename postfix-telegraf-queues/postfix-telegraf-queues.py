#!/usr/bin/env python
'''
    Still on Python 2 :(

    Script tries to mimic the postfix telegraf plugin from:
    https://github.com/influxdata/telegraf/pull/2553/commits/
'''
import subprocess
import os
import time
from os.path import getsize

def get_queue_directory():
    '''Gather information from Postfix'''
    result = subprocess.check_output(['postconf', '-h', 'queue_directory'])
    result = result.strip()
    if os.path.isfile(result):
        return result
    else:
        return "/var/spool/postfix"

def scan_directory(directory, qdir):
    '''Get information about directory'''
    ddir = QUEUE_DIR + "/" + directory
    count_files = 0
    count_size = 0
    age = 0
    mod_values = []
    for dirpath, dirnames, filenames in os.walk(ddir):
        count_files = count_files + len(filenames)
        for name in filenames:
            mod_values.append(int(os.stat(dirpath + "/" + name).st_mtime))
            count_size = count_size + getsize(dirpath + "/" + name)
    age = RIGHTNOW - min(mod_values or [0])
    if age == RIGHTNOW or age < 0:
        age = 0
    return directory, count_files, count_size, age

if __name__ == "__main__":
    QUEUE_DIR = get_queue_directory()
    RIGHTNOW = int(time.time())
    TEMP = 'postfix_queue,queue={0} length={1},size={2},age={3}'
    for directory in ['active', 'hold', 'incoming', 'maildrop', 'deferred']:
        print TEMP.format(*scan_directory(directory, QUEUE_DIR))
else:
    print "This script is not meant to be loaded"
