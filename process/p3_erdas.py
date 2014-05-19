#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from datetime import datetime
import os
from pymodis import convertmodis, parsemodis

import shutil
from lib import datetime_format


def run(config_run):

    if config_run.p3_erdas not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    #if not os.path.isdir(dir_process):
    #    os.makedirs(dir_process)
    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    # copying the directory
    try:
        shutil.copytree(os.path.join(config_run.abs_path_dir, 'p2_mrt'), dir_process)
    except OSError as error:
        msg = '\nError copying directory:\n' + str(error)
        config_run.process_logfile.write(msg+'\n')
        print msg
        return

    # finishing the process
    msg = '\nThe process {0} completed - ({1})'.format(config_run.process_name, datetime_format(datetime.today()))
    config_run.process_logfile.write(msg+'\n')
    print msg
    # save in setting
    config_run.p3_erdas = 'done - '+datetime_format(datetime.today())
    config_run.save()
