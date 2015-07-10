#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

from datetime import datetime
import os

import shutil
from lib import datetime_format


def run(config_run):

    if config_run.p1_tiseg not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    source_path = os.path.join(config_run.abs_path_dir, 'p0_download')

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.'.format(source_path)
        config_run.process_logfile.write(msg+'\n')
        print msg
        # save in setting
        config_run.p1_tiseg = 'with errors! - '+datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)
    if not os.path.isdir(dir_process):
        os.makedirs(dir_process)

    # copiar los archivos xml por escena y modo (A y Q), y generar la nomenclatura
    # para guardar los archivos de Tiseq por banda, los archivos xml son necesarios
    # para el siguiente proceso (MRT)
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.xml']
            if files:
                scene = os.path.basename(root)
                mode = os.path.basename(os.path.dirname(root))

                msg = 'Copying xml file in Tiseq process for {0} in scene {1}'.format(mode, scene)
                config_run.process_logfile.write(msg+'\n')
                print msg

                if mode == 'MOD09A1':
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoA_banda03.hdf.xml'))
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoA_banda04.hdf.xml'))
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoA_banda05.hdf.xml'))
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoA_banda06.hdf.xml'))
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoA_banda07.hdf.xml'))
                if mode == 'MOD09Q1':
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoQ_banda01.hdf.xml'))
                    shutil.copyfile(os.path.join(root, files[0]), os.path.join(dir_process, scene+'_modoQ_banda02.hdf.xml'))

    # finishing the process
    msg = '\nThe process {0} completed - ({1})'.format(config_run.process_name, datetime_format(datetime.today()))
    config_run.process_logfile.write(msg+'\n')
    print msg
    # save in setting
    config_run.p1_tiseg = 'done - '+datetime_format(datetime.today())
    config_run.save()