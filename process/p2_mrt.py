#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import os
import shutil
from subprocess import call
from datetime import datetime
from pymodis import convertmodis, parsemodis

from ATD.lib import datetime_format


def run(config_run):
    if config_run.p2_mrt not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg + '\n')
        print msg

    source_path = os.path.join(config_run.working_directory, 'p1_tiseg')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

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

    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x.endswith(('.hdf', '.HDF'))]
            for file in files:
                hdf_file = os.path.join(root, file)
                dest = os.path.dirname(os.path.join(dir_process, os.path.join(root, file).split('/p1_tiseg/')[-1]))

                msg = 'Processing file {0} with MRT: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print msg

                return_code, msg = modis_convert(hdf_file, dest)
                # if 'was converted successfully' in msg:
                #    msg = 'was converted successfully'
                config_run.process_logfile.write(msg + '\n')
                print msg

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print msg
    # save in setting
    config_run.p2_mrt = 'with errors! - ' if return_code != 0 else 'done - ' + datetime_format(datetime.today())
    config_run.save()


def modis_convert(hdf_file, dest):
    if not os.path.isdir(dest):
        os.makedirs(dest)

    # temporal directory for process files with mrt
    os.chdir(os.path.dirname(dest))
    mrt_dir_process = '.p2_mrt_tmp'
    # primero eliminar antes de crearla si existe
    if os.path.isdir(mrt_dir_process):
        shutil.rmtree(mrt_dir_process)
    if not os.path.isdir(mrt_dir_process):
        os.makedirs(mrt_dir_process)

    tmp_out_file = os.path.join(mrt_dir_process, os.path.basename(hdf_file).replace('.hdf', '.tif'))

    out_file = os.path.join(dest, os.path.basename(hdf_file)).replace('.hdf', '.tif')

    # opciones del remuestreo de la herramienta MRT
    options = {'subset': '( 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 )',
               'pp': '( 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 )',
               'pt': 'UTM',
               'mrt': os.environ["MRT_HOME"],
               'res': None,
               'resampl': 'NEAREST_NEIGHBOR',
               'datum': 'WGS84',
               'utm': '18',
               'output': tmp_out_file}

    modisParse = parsemodis.parseModis(hdf_file)
    confname = modisParse.confResample(options['subset'], options['res'],
                                       options['output'], options['datum'],
                                       options['resampl'], options['pt'],
                                       options['utm'], options['pp'])
    modisConver = convertmodis.convertModis(hdf_file, confname, options['mrt'])
    msg = modisConver.run()

    if 'was converted successfully' not in msg:
        # delete tmp dir
        if os.path.isdir(mrt_dir_process):
            shutil.rmtree(mrt_dir_process)
        return 1, msg

    ####### unir todas las bandas GeoTiff reproyectadas por cada imagen a GeoTiff multibanda

    # buscar todas las bandas dentro del directorio temporal
    input_all_band = []
    for root, dirs, files in os.walk(os.path.dirname(tmp_out_file)):
        if len(files) != 0:
            input_all_band = [os.path.join(root, x) for x in files if x[-4::] == '.tif']

    # ordenarlas segun las fechas de las bandas
    input_all_band = sorted(input_all_band)

    # combinacion de bandas a GeoTiff multibanda usando gdal
    return_code = call(["gdal_merge.py", "-o", out_file, "-of", "GTiff", "-separate"] + input_all_band)

    if return_code == 0:  # successfully
        msg = 'was converted successfully'
    else:
        msg = '\nError: Problem generating the output GeoTiff multiband\n' \
              'with gdal merge tool.'

    # delete tmp dir
    if os.path.isdir(mrt_dir_process):
        shutil.rmtree(mrt_dir_process)

    return return_code, msg
