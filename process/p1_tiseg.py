#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import os

import shutil

def run(config_run):

    if config_run.p1_tiseg not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    # copying the directory
    try:
        shutil.copytree(os.path.join(config_run.abs_path_dir, 'p0_download'),
                        os.path.join(config_run.abs_path_dir, config_run.process_name))
    except OSError as error:
        msg = '\nError copying directory:\n' + str(error)
        config_run.process_logfile.write(msg+'\n')
        print msg
        return