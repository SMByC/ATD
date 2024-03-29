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
from pymodis import convertmodis, parsemodis

from atd.lib import datetime_format


def run(config_run):
    if config_run.p1_mrt not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p0_download')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p1_mrt = 'with errors! - ' + datetime_format(datetime.today())
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
                dest = os.path.dirname(os.path.join(dir_process, os.path.join(root, file).split('/p0_download/')[-1]))

                msg = 'Processing file {0} with MRT: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print(msg, flush=True)

                # converting and making the layerstack of mrt tmp process
                return_code, msg = modis_convert(hdf_file, dest)
                config_run.process_logfile.write(msg + '\n')
                print(msg)

                # copy the xml
                xml_from = hdf_file + '.xml'
                xml_to = os.path.join(dest, os.path.basename(hdf_file) + '.xml')
                shutil.copy(xml_from, xml_to)

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p1_mrt = 'with errors! - ' if return_code != 0 else 'done - ' + datetime_format(datetime.today())
    config_run.save()


def modis_convert(hdf_file, dest):
    if not os.path.isdir(dest):
        os.makedirs(dest)

    out_file = os.path.join(dest, os.path.basename(hdf_file))

    # opciones del remuestreo de la herramienta MRT
    options = {'subset': '( 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 )',
               'pp': '( 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 )',
               'pt': 'UTM',
               'mrt': os.environ["MRT_HOME"],
               'res': None,
               'resampl': 'NEAREST_NEIGHBOR',
               'datum': 'WGS84',
               'utm': '18',
               'output': out_file}

    modisParse = parsemodis.parseModis(hdf_file)
    confname = modisParse.confResample(options['subset'], options['res'],
                                       options['output'], options['datum'],
                                       options['resampl'], options['pt'],
                                       options['utm'], options['pp'])
    modisConver = convertmodis.convertModis(hdf_file, confname, options['mrt'])
    msg = modisConver.run()

    if 'was converted successfully' not in msg:
        # delete tmp dir
        return 1, msg

    return 0, msg
