#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from datetime import datetime
import os

import shutil
from lib import datetime_format


def run(config_run):

    if config_run.p1_tiseg not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    # copying the directory
    try:
        shutil.copytree(os.path.join(config_run.abs_path_dir, 'p0_download'), dir_process)
    except OSError as error:
        msg = '\nError copying directory:\n' + str(error)
        config_run.process_logfile.write(msg+'\n')
        print msg
        return

    # clear some log files
    if os.path.isfile(os.path.join(dir_process, 'download.log')):
        os.remove(os.path.join(dir_process, 'download.log'))
    if os.path.isfile(os.path.join(dir_process,'MOD09A1', 'MOD09A1_status.csv')):
        os.remove(os.path.join(dir_process,'MOD09A1', 'MOD09A1_status.csv'))
    if os.path.isfile(os.path.join(dir_process,'MOD09Q1', 'MOD09Q1_status.csv')):
        os.remove(os.path.join(dir_process,'MOD09Q1', 'MOD09Q1_status.csv'))

    # finishing the process
    msg = '\nThe process {0} completed - ({1})'.format(config_run.process_name, datetime_format(datetime.today()))
    config_run.process_logfile.write(msg+'\n')
    print msg
    # save in setting
    config_run.p1_tiseg = 'done - '+datetime_format(datetime.today())
    config_run.save()