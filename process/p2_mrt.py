#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from datetime import datetime
import os
from pymodis import convertmodis, parsemodis

import shutil
from lib import datetime_format


def run(config_run):

    if config_run.p2_mrt not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    source_path = os.path.join(config_run.abs_path_dir, 'p1_tiseg')

    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.hdf']
            for file in files:
                hdf_file = os.path.join(root, file)
                dest = os.path.dirname(os.path.join(dir_process, os.path.join(root, file).split('/p1_tiseg/')[-1]))

                msg = 'Processing file {0} with MRT: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print msg

                msg = modis_convert(hdf_file, dest)
                if 'was converted successfully' in msg:
                    msg = 'was converted successfully'
                config_run.process_logfile.write(msg+'\n')
                print msg

    # finishing the process
    msg = '\nThe process {0} completed - ({1})'.format(config_run.process_name, datetime_format(datetime.today()))
    config_run.process_logfile.write(msg+'\n')
    print msg
    # save in setting
    config_run.p2_mrt = 'done - '+datetime_format(datetime.today())
    config_run.save()


def modis_convert(hdf_file, dest):

    if not os.path.isdir(dest):
        os.makedirs(dest)

    # create a temporal directory for process the file with mrt
    mrt_dir_process = '.tmp_mrt'
    if not os.path.isdir(mrt_dir_process):
        os.makedirs(mrt_dir_process)

    options = {'subset': '( 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 )',
               'pp': '( 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 )',
               'pt': 'UTM',
               'mrt': '/multimedia/Tmp_build/MRT_download_Linux64/MRT',
               'res': None,
               'resampl': 'NEAREST_NEIGHBOR',
               'datum': 'WGS84',
               'utm': '18',
               'output': os.path.join(mrt_dir_process, os.path.basename(hdf_file))}

    modisParse = parsemodis.parseModis(hdf_file)
    confname = modisParse.confResample(options['subset'], options['res'],
                                       options['output'], options['datum'],
                                       options['resampl'], options['pt'],
                                       options['utm'], options['pp'])
    modisConver = convertmodis.convertModis(hdf_file, confname, options['mrt'])
    msg = modisConver.run()

    # move to dest
    if os.path.isfile(os.path.join(dest, os.path.basename(hdf_file))):
        os.remove(os.path.join(dest, os.path.basename(hdf_file)))
    shutil.move(os.path.join(mrt_dir_process, os.path.basename(hdf_file)), dest)

    # delete tmp dir
    if os.path.isdir(mrt_dir_process):
        shutil.rmtree(mrt_dir_process)

    return msg
