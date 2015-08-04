#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

from datetime import datetime
import os
# from pymodis import convertmodis, parsemodis

import shutil
from subprocess import call
from lib import datetime_format


def run(config_run):
    if config_run.p2_mrt not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg + '\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    source_path = os.path.join(config_run.abs_path_dir, 'p5_nodata')

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.'.format(source_path)
        config_run.process_logfile.write(msg + '\n')
        print msg
        # save in setting
        config_run.p2_mrt = 'with errors! - ' + datetime_format(datetime.today())
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

    # recorre los archivos tif de la carpeta del proceso anterior (p5_nodata)
    # detecta y reagrupa todas las imagenes de una misma banda y modo, seleccionando
    # todas las escenas de esta banda para generar y crear el mosaico con Gdal
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.tif']
            if files:
                var = os.path.basename(root)

                imgs_group_by_var[var] = []
                imgs_by_scene = []

                files_temp_list = list(files)
                while files_temp_list:
                    # obtener la parte del nombre que se repite por cada variable
                    # para seleccionar los archivos para el mosaico
                    file_group = files_temp_list[0]
                    for group_name in scenes:
                        if group_name in os.path.basename(file_group):
                            scene_group_name = file_group.split(group_name)[1]

                    # seleccionar los archivos para el mosaico
                    mosaic_input_list = []
                    for file in files_temp_list:
                        if scene_group_name in file:
                            mosaic_input_list.append(file)
                    # nombre de salida del mosaico
                    scene_group_name = scene_group_name.replace(var, '')
                    scene_group_name = scene_group_name.replace('.tif', '')
                    scene_group_name = scene_group_name[0:-1] if scene_group_name[-1] == '_' else scene_group_name
                    scene_group_name = scene_group_name[1::] if scene_group_name[0] == '_' else scene_group_name
                    mosaic_name = scene_group_name + '_mosaico_' + var + '.tif'
                    # ruta del archivo del mosaico
                    mosaic_dest = os.path.dirname(
                        os.path.join(dir_process, os.path.join(root, file).split('/p5_nodata/')[-1]))
                    # lista de archivos para el mosaico con ruta absoluta
                    mosaic_input_list_fullpath = [os.path.join(root, f) for f in mosaic_input_list]

                    # Procesar el mosaico
                    msg = 'Processing mosaic {0}: '.format(mosaic_name)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print msg
                    # generar el mosaico
                    return_code, msg = mosaic(mosaic_input_list_fullpath, mosaic_dest, mosaic_name)
                    config_run.process_logfile.write(msg + '\n')
                    print msg

                    # quitar los archivos procesados en la lista y ordenarlos
                    files_temp_list = sorted(list(set(files_temp_list) - set(mosaic_input_list)))

                    if return_code != 0:
                        break

                if return_code != 0:
                    break

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))
    config_run.process_logfile.write(msg + '\n')
    print msg
    # save in setting
    config_run.p6_mosaic = 'with errors! - ' if return_code != 0 else 'done - ' + datetime_format(datetime.today())
    config_run.save()


def mosaic(mosaic_input_list, mosaic_dest, mosaic_name):
    if not os.path.isdir(mosaic_dest):
        os.makedirs(mosaic_dest)

    out_file = os.path.join(mosaic_dest, mosaic_name)

    # generar el mosaico con el programa gdalwarp de gdal
    # gdalwarp salida_h1* m.tif -srcnodata 0 -dstnodata 255
    return_code = call(["gdalwarp"] + mosaic_input_list + [out_file, '-srcnodata', '0', '-dstnodata', '255'])

    if return_code == 0:  # successfully
        msg = 'mosaic created successfully'
    else:
        msg = '\nError: The mosaic can not create for any reason, please check\n' \
              'error message above, likely the files not were\n' \
              'processed successfully.'

    return return_code, msg
