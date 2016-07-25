#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import sys
import argparse
import time
import warnings
from datetime import datetime

from atd import settings
from atd.lib import datetime_format
from atd.download import main as download_main
from atd.process import p1_mrt, p2_qc4sd, p3_mosaic, p4_stats, p5_layerstack


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
    epilog="Xavier Corredor Llano <xcorredorl@ideam.gov.co>\n"
           "Sistema de Monitoreo de Bosques y Carbono - SMBYC\n"
           "IDEAM, Colombia",
    formatter_class=argparse.RawTextHelpFormatter,)

subparsers = parser.add_subparsers(dest='make', help='operation to be performed')

# DOWNLOAD
group_download = subparsers.add_parser('download', help='download modis files')
group_download.add_argument('--source', type=str, dest='source', default=None,
                            help='satellite source of MODIS, terra or aqua')
group_download.add_argument('--from', type=str, dest='from_date',
                            help='date from download modis files, format: y-m-d')  # else None
group_download.add_argument('--to', type=str, dest='to_date',
                            help='date to download modis files, format: y-m-d')  # else None
group_download.add_argument('--type', type=str, dest='download_type', choices={'steps', 'full'},
                            help='type of download, steps or full')  # else None
group_download.add_argument('--email', type=str, help='send email when finnish')
group_download.add_argument('working_directory', help='working directory to process', action=readable_dir,
                            nargs='?', default=os.getcwd())

# PROCESS
list_of_process = ['p1_mrt', 'p2_qc4sd', 'p3_mosaic', 'p4_stats', 'p5_layerstack']
group_process = subparsers.add_parser('process', help='process {0}'.format(','.join(list_of_process)))
group_process.add_argument('process', type=str, choices=list_of_process,
                           help='process {0}'.format(','.join(list_of_process)))
group_process.add_argument('--email', type=str, help='send email when finnish')
group_process.add_argument('working_directory', help='working directory to process', action=readable_dir)
# only for p4_stats for computes statistics
group_process.add_argument('--prev_rundir', help='directory of the previous run (only for run p4_stats)',
                           action=readable_dir)
group_process.add_argument('--np', dest='number_of_processes', type=int, help='number of processes',
                           required=False)
group_process.add_argument('--tmp_dir', type=str, default=None,
                           help='temporal directory for cache', required=False)

# print help if not pass arguments
if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)

# process arguments
args = parser.parse_args()

####################################### set/get settings ######################################

config_run = settings.get(args)

########################################## download ###########################################

if args.make == 'download':
    # save make in config_run
    config_run.make = args.make
    # download
    download_main.run(config_run)

########################################### process ###########################################

if args.make == 'process':
    # ignore warnings
    warnings.filterwarnings("ignore")
    # save make in config_run
    config_run.make = args.make
    # set the log file for process
    config_run.process_logfile = open(os.path.join(config_run.working_directory, 'process.log'), 'a')
    # init log of process
    msg = '\n\n########### START LOG FOR PROCESS: ' + args.process + \
          ' - (' + datetime_format(datetime.today()) + ') ###########' + \
          '\n#### in dir: ' + os.path.basename(config_run.working_directory) + '\n'
    config_run.process_logfile.write(msg + '\n')
    print(msg, flush=True)

    config_run.process_name = args.process

    ######################################### MRT process #########################################
    if args.process == 'p1_mrt':
        p1_mrt.run(config_run)

    ############################### Quality Control process - QC4SD ###############################
    if args.process == 'p2_qc4sd':
        p2_qc4sd.run(config_run)

    ######################################### make mosaic #########################################
    if args.process == 'p3_mosaic':
        p3_mosaic.run(config_run)

    ##################################### statistics process ######################################
    if args.process == 'p4_stats':
        p4_stats.run(config_run)

    ################################### create the layer stack ####################################
    if args.process == 'p5_layerstack':
        p5_layerstack.run(config_run)

print('\nFinish')

exit()
