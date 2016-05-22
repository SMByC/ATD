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

from atd.lib import datetime_format


def run(config_run):
    if config_run.p2_reproj not in [None, 'None']:
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
        config_run.p2_reproj = 'with errors! - ' + datetime_format(datetime.today())
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
                msg = 'Reprojection file {0} with Gdal: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print(msg)

                ##############
                # first reprojection to The WGS LatLon (EPSG:4326)
                in_file = os.path.join(root, file)
                file_tmp = "tmp_"+file
                out_file_tmp = os.path.join(dir_process, file_tmp)

                # Reprojection with Gdal
                return_code = call(["gdalwarp", in_file, out_file_tmp, "-r", "near", "-t_srs", "EPSG:4326"])

                if return_code == 0:  # successfully
                    msg = 'was reprojected to EPSG:4326 successfully'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                else:
                    msg = '\nError: Problem with reprojecting in Gdal\n'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                    break

                ##############
                # second reprojection to WGS 84 / UTM zone 18N (EPSG:32618)
                out_file = os.path.join(dir_process, file)
                mode = file.split('_')[1]

                # process the mode MXD09A1: adjust pixel size
                if mode in ['MOD09A1', 'MYD09A1']:
                    pixel_size = ["500","500"]
                # process the mode MXD09Q1: adjust pixel size
                if mode in ['MOD09Q1', 'MYD09Q1']:
                    pixel_size = ["250", "250"]

                # Reprojection with Gdal
                return_code = call(["gdalwarp", out_file_tmp, out_file, "-r", "near", "-t_srs", "EPSG:32618",
                                    "-co", "COMPRESS=LZW", "-co", "PREDICTOR=2", "-co", "TILED=YES", "-tr"] + pixel_size)

                if return_code == 0:  # successfully
                    msg = 'was reprojected to EPSG:32618 successfully'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                else:
                    msg = '\nError: Problem with reprojecting in Gdal\n'
                    config_run.process_logfile.write(msg + '\n')
                    print(msg)
                    break

                os.remove(out_file_tmp)

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p2_reproj = 'with errors! - ' if return_code != 0 else 'done - ' + datetime_format(datetime.today())
    config_run.save()
