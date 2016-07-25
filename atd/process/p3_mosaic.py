#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import shutil
from datetime import datetime
from subprocess import call

from atd.lib import datetime_format


def run(config_run):
    if config_run.p3_mosaic not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p2_qc4sd')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p3_mosaic = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    imgs_group_by_var = {}

    scenes = [
        'h10v07',
        'h10v08',
        'h10v09',
        'h11v07',
        'h11v08',
        'h11v09',
    ]

    # process file by file detecting the groups of bands and subproducts data
    # inside directory for make the mosaic based on the name of the directory
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.tif']
            if files:
                var = os.path.basename(root)

                imgs_group_by_var[var] = []
                imgs_by_scene = []

                files_temp_list = list(files)
                while files_temp_list:
                    # detect the repeat name for each variable for process the group
                    file_group = files_temp_list[0]
                    for group_name in scenes:
                        if group_name in os.path.basename(file_group):
                            scene_group_name = file_group.split(group_name)[1]

                    # selecting the file for mosaic
                    mosaic_input_list = []
                    for file in files_temp_list:
                        if scene_group_name in file:
                            mosaic_input_list.append(file)
                    # output name file for mosaic
                    scene_group_name = scene_group_name.replace(var, '')
                    scene_group_name = scene_group_name.replace('.tif', '')
                    scene_group_name = scene_group_name[0:-1] if scene_group_name[-1] == '_' else scene_group_name
                    scene_group_name = scene_group_name[1::] if scene_group_name[0] == '_' else scene_group_name
                    # define file names
                    mosaic_name_tmp = scene_group_name + '_tmp.tif'
                    mosaic_name = scene_group_name + '.tif'
                    # mosaic path
                    mosaic_dest = os.path.dirname(
                        os.path.join(dir_process, os.path.join(root, file).split('/p2_qc4sd/')[-1]))
                    # list of file for make mosaic
                    mosaic_input_list_fullpath = [os.path.join(root, f) for f in mosaic_input_list]

                    # mosaic process
                    msg = 'Processing mosaic {0}: '.format(mosaic_name)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, flush=True)
                    return_code, msg = mosaic(mosaic_input_list_fullpath, mosaic_dest, mosaic_name_tmp)
                    config_run.process_logfile.write(msg + '\n')
                    print(msg, flush=True)
                    if return_code != 0: break

                    # clipping Colombia shape
                    return_code, msg = clipping_colombia(os.path.join(mosaic_dest, mosaic_name_tmp),
                                                         os.path.join(mosaic_dest, mosaic_name))
                    config_run.process_logfile.write(msg + '\n')
                    print(msg, flush=True)
                    if return_code != 0: break

                    # clean process files in list and sorted
                    files_temp_list = sorted(list(set(files_temp_list) - set(mosaic_input_list)))
                    os.remove(os.path.join(mosaic_dest, mosaic_name_tmp))

                if return_code != 0: break

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))
    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p3_mosaic = ('with errors! - ' if return_code != 0 else 'done - ') + datetime_format(datetime.today())
    config_run.save()


def mosaic(mosaic_input_list, mosaic_dest, mosaic_name):
    if not os.path.isdir(mosaic_dest):
        os.makedirs(mosaic_dest)

    out_file = os.path.join(mosaic_dest, mosaic_name)

    # make mosaic with gdal
    # gdalwarp salida_h1* m.tif -srcnodata 0 -dstnodata 255
    return_code = call(["gdalwarp"] + mosaic_input_list + [out_file])

    if return_code == 0:  # successfully
        msg = 'mosaic created successfully'
    else:
        msg = '\nError: The mosaic can not create for any reason, please check\n' \
              'error message above, likely the files not were\n' \
              'processed successfully.'

    return return_code, msg

def clipping_colombia(infile, outfile):
    base_dir = os.path.dirname(__file__)

    colombia_buffer_shape = os.path.join(base_dir, "shapes", "Colombia", "Colombia_WGS84_Z18_Buffer_1200m.shp")

    return_code = call(["gdalwarp", "-cutline", colombia_buffer_shape, infile, outfile,
                        "-co", "COMPRESS=LZW", "-co", "PREDICTOR=2", "-co", "TILED=YES"])

    if return_code == 0:  # successfully
        msg = 'clipping with Colombia shape successfully'
    else:
        msg = '\nError: The clipping with Colombia shape can not processed for any reason, please check\n' \
              'error message above, likely the files not were\n' \
              'processed successfully.'

    return return_code, msg