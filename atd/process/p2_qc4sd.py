#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

from datetime import datetime
import os

import shutil
from atd.lib import datetime_format
from qc4sd import qc4sd


def run(config_run):
    if config_run.p2_qc4sd not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p1_mrt')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p2_qc4sd = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)
    if not os.path.isdir(dir_process):
        os.makedirs(dir_process)

    # process the time series (in the whole period) with QC4SD by tile and by mode
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x.endswith(('.hdf', '.HDF'))]
            if files:
                tile = os.path.basename(root)
                mode = os.path.basename(os.path.dirname(root))
                files = sorted(files)

                msg = 'Making the quality control process (using QC4SD) for {0} in tile {1}:'.format(mode, tile)
                config_run.process_logfile.write(msg + '\n')
                print(msg)

                files_with_path = [os.path.join(root, file) for file in files]

                # process the mode MXD09A1 for the bands 3,4,5,6 and 7
                if mode in ['MOD09A1', 'MYD09A1']:
                    qc4sd.run('default', [3, 4, 5, 6, 7], files_with_path, dir_process)
                # process the mode MXD09Q1 for the bands 1 and 2
                if mode in ['MOD09Q1', 'MYD09Q1']:
                    qc4sd.run('default', [1, 2], files_with_path, dir_process)

    # finishing the process
    msg = '\nThe process {0} completed - ({1})'.format(config_run.process_name, datetime_format(datetime.today()))
    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p2_qc4sd = 'done - ' + datetime_format(datetime.today())
    config_run.save()
