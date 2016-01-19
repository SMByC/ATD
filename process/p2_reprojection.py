#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import shutil
from subprocess import call
from datetime import datetime

from ATD.lib import datetime_format


def run(config_run):
    if config_run.p2_reprojection not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p1_qc4sd')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p2_reprojection = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)
    if not os.path.isdir(dir_process):
        os.makedirs(dir_process)

    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x.endswith(('.tif', '.TIF'))]
            for file in files:
                in_file = os.path.join(root, file)
                out_file = os.path.join(dir_process, file)

                msg = 'Reprojection file {0} with Gdal: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print(msg)

                # Reprojection with Gdal
                # gdalwarp h10v08_MOD09A1_band07.tif h10v08_MOD09A1_reproj.tif -r near -t_srs EPSG:32618
                return_code = call(["gdalwarp", in_file, out_file, "-r", "near", "-t_srs", 'EPSG:32618'])

                if return_code == 0:  # successfully
                    msg = 'was reprojected successfully'
                else:
                    msg = '\nError: Problem with reprojecting in Gdal\n'

                if return_code == 0:  # successfully
                    msg = 'was reprojected successfully'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                else:
                    msg = '\nError: Problems reprojecting in Gdal\n'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                    break

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p2_reprojection = 'with errors! - ' if return_code != 0 else 'done - ' + datetime_format(datetime.today())
    config_run.save()
