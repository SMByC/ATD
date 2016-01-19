#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import sys
from datetime import datetime
from subprocess import call
from time import sleep

gfs_path = "/state/partition1/RocksCluster/main/input_data/gfs/"

run_path = os.path.dirname(os.path.realpath(__file__))

log = open(os.path.join(run_path, 'daemon_run.log'), 'w')


def datetime_format(dt):
    return dt.strftime("%Y/%m/%d %H:%M")


def fix_zeros_in_datetime(dt):
    return '0' + str(dt) if len(str(dt)) < 2 else str(dt)


for attempt in range(50):

    hour = sys.argv[1]

    horizon = sys.argv[2]

    today = datetime.today()

    file_path = os.path.join(gfs_path, "{year}{month}{day}" \
                             .format(year=today.year, month=fix_zeros_in_datetime(today.month),
                                     day=fix_zeros_in_datetime(today.day)), hour, "download_status.csv")

    if os.path.isfile(file_path):
        log.write('ready to run - ' + datetime_format(datetime.today()) + '\n')
        log.flush()
        run_status = call(['sh', '/state/partition1/RocksCluster/main/main.sh', hour, horizon], shell=False)
        log.write('run finished - ' + datetime_format(datetime.today()) + '\n')
        log.write('run status: ' + str(run_status) + '\n')
        log.flush()
        log.close()
        exit()

    log.write(
        'waiting for input_data available (attempt {0}) - '.format(attempt) + datetime_format(datetime.today()) + '\n')
    log.write('\t' + file_path + '\n')
    log.flush()
    sleep(300)
