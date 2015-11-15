#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import os
import sys
import argparse
import time
from datetime import datetime

from ATD import settings
from ATD.lib import datetime_format
from ATD.download import main as download_main
from ATD.process import p1_tiseg, p2_mrt, pN_nodata, p4_stats, p6_mosaic, p7_layerstack


# set encoding to utf-8
reload(sys)
sys.setdefaultencoding("utf-8")


########################################## arguments ##########################################

def mkdate(datestr):
    return time.strptime(datestr, '%Y-%m-%d')


class readable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = values
        if not os.path.isdir(prospective_dir):
            raise argparse.ArgumentTypeError("readable_dir:{0} is not a valid path".format(prospective_dir))
        if os.access(prospective_dir, os.R_OK):
            setattr(namespace, self.dest, prospective_dir)
        else:
            raise argparse.ArgumentTypeError("readable_dir:{0} is not a readable dir".format(prospective_dir))


# Create parser arguments
parser = argparse.ArgumentParser(
    prog='atd',
    description="Alertas Tempranas de Deforestacion",
    formatter_class=argparse.RawTextHelpFormatter)

subparsers = parser.add_subparsers(dest='make', help='operation to be performed')

# DOWNLOAD
group_download = subparsers.add_parser('download', help='download modis files')
group_download.add_argument('--source', type=str, dest='source', default='terra,aqua',
                            help='satellite source of MODIS, terra or aqua')
group_download.add_argument('--from', type=str, dest='from_date',
                            help='date from download modis files, format: y-m-d')  # else None
group_download.add_argument('--to', type=str, dest='to_date',
                            help='date to download modis files, format: y-m-d')  # else None
group_download.add_argument('--type', type=str, dest='download_type', choices={'steps', 'full'},
                            help='type of download, steps or full')  # else None
group_download.add_argument('path', help='path to download modis files', action=readable_dir, nargs='?',
                            default=os.getcwd())  # else current directory
group_download.add_argument('--email', type=str, help='send email when finnish')

# PROCESS
list_of_process = ['p1_tiseg', 'p2_mrt', 'p3_nodata', 'p4_stats', 'p5_nodata', 'p6_mosaic', 'p7_layerstack']
group_process = subparsers.add_parser('process', help='process {0}'.format(','.join(list_of_process)))
group_process.add_argument('process', type=str, choices=list_of_process,
                           help='process {0}'.format(','.join(list_of_process)))
group_process.add_argument('folder', type=str, action=readable_dir, help='folder to process')
group_process.add_argument('--email', type=str, help='send email when finnish')

args = parser.parse_args()

####################################### set/get settings ######################################

config_run = settings.get(args)

########################################## download ###########################################

if args.make == 'download':
    for source in config_run.source:
        # save make in config_run
        config_run.make = args.make
        # save the current source to download
        config_run.current_source = source

        download_main.run(config_run)

########################################### process ###########################################

if args.make == 'process':
    # save make in config_run
    config_run.make = args.make
    # set the log file for process
    config_run.process_logfile = open(os.path.join(config_run.abs_path_dir, 'process.log'), 'a')
    # init log of process
    msg = '\n\n########### START LOG FOR PROCESS: ' + args.process + ' - (' + datetime_format(
        datetime.today()) + ') ###########'
    config_run.process_logfile.write(msg + '\n')
    print msg

    config_run.process_name = args.process

    ######################################## TiSeg process ########################################
    if args.process == 'p1_tiseg':
        p1_tiseg.run(config_run)

    ######################################### MRT process #########################################
    if args.process == 'p2_mrt':
        p2_mrt.run(config_run)

    ############################ convert negative data to zero - nodata ###########################
    if args.process == 'p3_nodata':
        pN_nodata.run(config_run, 'p3_nodata')

    #################################### statistics R process #####################################
    if args.process == 'p4_stats':
        p4_stats.run(config_run)

    ############################ convert negative data to zero - nodata ###########################
    if args.process == 'p5_nodata':
        pN_nodata.run(config_run, 'p5_nodata')

    ######################################### make mosaic #########################################
    if args.process == 'p6_mosaic':
        p6_mosaic.run(config_run)

    ################################### create the layer stack ####################################
    if args.process == 'p7_layerstack':
        p7_layerstack.run(config_run)

print '\nFinish'

exit()
