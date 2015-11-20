#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2015
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import shutil
from subprocess import call
from datetime import datetime

from ATD.lib import datetime_format, get_pixel_size


def run(config_run):
    if config_run.p7_layerstack not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print msg

    source_path = os.path.join(config_run.working_directory, 'p6_mosaic')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print msg
        # save in setting
        config_run.p7_layerstack = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    ####### unir todas las bandas GeoTiff reproyectadas por cada imagen a GeoTiff multibanda

    sorted_bands = [0, 1, 4, 5, 6, 2, 3]

    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.tif']
            if files:
                var = os.path.basename(root)
                msg = 'Generating layer stack for variable {0}: '.format(var)
                config_run.process_logfile.write(msg + '\n')
                print msg

                dest = os.path.join(dir_process, var)
                if not os.path.isdir(dest):
                    os.makedirs(dest)

                input_all_band = sorted(files)
                # ordenar las bandas segun sorted_bands
                input_all_band = [x for (y, x) in sorted(zip(sorted_bands, input_all_band))]
                input_all_band = [os.path.join(root, x) for x in input_all_band]

                # get the lower pixel size of all input band
                all_pixel_sizes = [get_pixel_size(f) for f in input_all_band]
                lower_pixel_size = sorted(all_pixel_sizes)[0]
                msg = '  calculating the lower pixel size for all bands: ' + str(lower_pixel_size)
                config_run.process_logfile.write(msg + '\n')
                config_run.process_logfile.flush()
                print msg

                # nombre del layer stack
                out_file = os.path.join(dest, "LayerStack_" + var + ".tif")

                # combinacion de bandas a GeoTiff multibanda usando gdal
                return_code = call(
                    ["gdal_merge.py", "-o", out_file, "-of", "GTiff", "-separate", "-ps", str(lower_pixel_size),
                     str(lower_pixel_size)] + input_all_band)

                if return_code == 0:  # successfully
                    msg = '  was converted successfully'
                    config_run.process_logfile.write(msg + '\n')
                    print msg
                else:
                    msg = '\nError: The R script return a error, please check\n' \
                          'error message above, likely the files not were\n' \
                          'processed successfully.'
                    config_run.process_logfile.write(msg + '\n')
                    print msg
                    break

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print msg
    # save in setting
    config_run.p7_layerstack = 'done - ' + datetime_format(datetime.today())
    config_run.save()
