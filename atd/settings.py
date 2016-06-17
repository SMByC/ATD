#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
from copy import deepcopy
from datetime import date
import configparser

from atd.lib import DateATD, dir_date_name


###############################################################################


class ConfigRun:
    def __init__(self, working_directory, make):
        self.working_directory = os.path.abspath(working_directory)
        self.make = make

        if self.make == "download":
            self.__init_download__()

        if self.make == "process":
            self.__init_process__()

    def __init_download__(self):
        ## [MAIN]
        self.source = None
        self.start_date = None
        self.target_date = None
        self.end_date = None
        ## [DOWNLOAD]
        self.download_type = None
        self.dnld_logfile = None
        self.dnld_errors = None
        self.dnld_finished = False

        ## TYPE MODIS TO DOWNLOAD
        # for Modis products: MOD09A1, MOD09Q1, MYD09A1, MYD09Q1
        # surface Reflectance 8-Day L3: http://modis.gsfc.nasa.gov/data/dataprod/mod09.php
        #self.modis_type = "mxd09a1_q1"
        #self.freq_time = 8  # 8-days

        # for Modis products: MOD09GA, MOD09GQ, MYD09GA, MYD09GQ
        # surface Reflectance Daily L2G: http://modis.gsfc.nasa.gov/data/dataprod/mod09.php
        self.modis_type = "mxd09ga_gq"
        self.freq_time = 1  # daily

        ## variables that not save into settings
        self.set_config_file()
        self.email = None
        self.download_path = None  # complete path to download (working_directory + 'p0_download')

    def __init_process__(self):
        ## [MAIN]
        self.source = None
        self.start_date = None
        self.end_date = None
        ## [PROCESS]
        self.list_of_process = ['p1_qc4sd', 'p2_reproj', 'p3_mosaic', 'p4_stats', 'p5_layerstack']
        for p in self.list_of_process:
            exec ('self.' + p + ' = None')
        # create the dictionary access process
        self.process_ = {'p1_qc4sd': self.p1_qc4sd, 'p2_reproj': self.p2_reproj,
                         'p3_mosaic': self.p3_mosaic, 'p4_stats': self.p4_stats,
                         'p5_layerstack': self.p5_layerstack}

        ## variables that not save into settings
        self.set_config_file()
        self.email = None

    def set_config_file(self):
        if self.make == "download":
            self.config_file = os.path.join(self.working_directory, 'download_settings.cfg')
        if self.make == "process":
            self.config_file = os.path.join(self.working_directory, 'process_settings.cfg')

    def load(self):
        config = configparser.RawConfigParser()
        if not os.path.isfile(self.config_file):
            return

        config.read(self.config_file)

        if self.make == "download":
            ## [MAIN]
            self.source = config.get('MAIN', 'source')
            self.start_date = config.get('MAIN', 'start_date')
            self.target_date = config.get('MAIN', 'target_date')
            self.end_date = config.get('MAIN', 'end_date')
            ## [DOWNLOAD]
            self.download_type = config.get('DOWNLOAD', 'download_type')
            self.dnld_errors = config.get('DOWNLOAD', 'dnld_errors')
            self.dnld_finished = config.get('DOWNLOAD', 'dnld_finished')

        if self.make == "process":
            ## [MAIN]
            self.source = config.get('MAIN', 'source')
            self.start_date = config.get('MAIN', 'start_date')
            self.end_date = config.get('MAIN', 'end_date')
            ## [PROCESS]
            for p in self.list_of_process:
                exec("self." + p + " = config.get('PROCESS', '" + p + "')")
            # create the dictionary access process
            self.process_ = {'p1_qc4sd': self.p1_qc4sd, 'p2_reproj': self.p2_reproj,
                             'p3_mosaic': self.p3_mosaic, 'p4_stats': self.p4_stats,
                             'p5_layerstack': self.p5_layerstack}

    def save(self):
        config = configparser.ConfigParser()

        if self.make == "download":
            config['MAIN'] = {}
            config['MAIN']['source'] = ','.join(self.source) if self.source not in [None, 'None'] else 'None'
            config['MAIN']['start_date'] = str(self.start_date)
            config['MAIN']['target_date'] = str(self.target_date)
            config['MAIN']['end_date'] = str(self.end_date)
            config['DOWNLOAD'] = {}
            config['DOWNLOAD']['download_type'] = self.download_type
            config['DOWNLOAD']['dnld_errors'] = ','.join(self.dnld_errors) if self.dnld_errors not in [None, 'None'] else 'None'
            config['DOWNLOAD']['dnld_finished'] = str(self.dnld_finished)

        if self.make == "process":
            config['MAIN'] = {}
            config['MAIN']['source'] = ','.join(self.source) if self.source not in [None, 'None'] else 'None'
            config['MAIN']['start_date'] = str(self.start_date)
            config['MAIN']['end_date'] = str(self.end_date)
            config['PROCESS'] = {}
            for p in self.list_of_process:
                exec("config['PROCESS']['" + p + "'] = str(self." + p + ")")

        # Writing our configuration file to 'example.cfg'
        with open(self.config_file, 'w') as configfile:
            config.write(configfile)

###############################################################################


def get(args):
    # global_path_to_run = '/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/'
    # global_path_to_run = '/Modelo_Raster/Modelos/Modelos1/Alertas_Temp_Deforest/'

    if args.make == 'download':

        config_run = ConfigRun(args.working_directory, args.make)
        if os.path.isfile(config_run.config_file):
            config_run.load()

        #########################
        ## loads all arguments and settings.cfg into config_run

        ## source
        if args.source not in [None, 'None']:
            if config_run.source is not None and sorted(args.source.split(',')) != sorted(config_run.source.split(',')):
                print("\nError: the source in download_settings.cfg and in arguments are different, if you\n" \
                      "want run other source, finished/delete the other source before run this.\n" \
                      "\tin argumets:     " + args.source +\
                      "\n\tin file settings: " + config_run.source)

                exit()
            args.source = args.source.split(',')
        else:
            args.source = ['terra','aqua']

        if config_run.source in [None, 'None']:
            config_run.source = args.source
        else:
            config_run.source = config_run.source.split(',')

        if config_run.source not in [['terra'], ['aqua'], ['terra','aqua'], ['aqua','terra']]:
            print("\nError: the source in argument is not recognized, this should be\n" \
                  "'terra', 'aqua' or 'terra,aqua'.")
            exit()

        ## start date
        if args.from_date is not None:
            config_run.start_date = DateATD(args.from_date, 'start', config_run.freq_time)
        elif config_run.start_date in [None, 'None']:
            today = date.today()
            config_run.start_date = DateATD(today.strftime('%Y-%m-%d'), 'start', config_run.freq_time)
        else:
            config_run.start_date = DateATD(config_run.start_date, 'start', config_run.freq_time)

        ## end date
        if args.to_date is not None:
            config_run.end_date = DateATD(args.to_date, 'end', config_run.freq_time)
        elif isinstance(config_run.end_date, str) and not config_run.end_date in [None, 'None']:
            config_run.end_date = DateATD(config_run.end_date, 'end', config_run.freq_time)

        ## target date
        if config_run.target_date in [None, 'None']:
            config_run.target_date = deepcopy(config_run.start_date)
        else:
            config_run.target_date = DateATD(config_run.target_date, config_run.freq_time)
            # test if target date are between the start and end date
            if not (config_run.start_date.date <= config_run.target_date.date):
                msg = '\nWarning: The start date is bigger than target date, changing\n' \
                      'the target date to start date'
                print(msg)
                config_run.target_date = deepcopy(config_run.start_date)

        ## fix name the working directory
        if config_run.working_directory == os.getcwd():
            config_run.working_directory = os.path.abspath(dir_date_name(config_run.start_date, config_run.target_date))
            msg = '\nWarning: The working directory is empty, not set in arguments, \n' \
                  'start new empty working directory base on dates of run:\n\n\t' + \
                  config_run.working_directory
            print(msg)
        elif os.path.basename(config_run.working_directory) != dir_date_name(config_run.start_date, config_run.target_date):
            old_working_directory = os.path.basename(config_run.working_directory)
            config_run.working_directory = os.path.abspath(dir_date_name(config_run.start_date, config_run.target_date))
            msg = '\nWarning: The current working directory not match with the start \n' \
                  'and target date parameters, renaming the working directory to:\n\n\t' + \
                  os.path.basename(old_working_directory) + '  -->  ' + os.path.basename(config_run.working_directory)
            print(msg)
            # rename directory
            os.rename(old_working_directory, config_run.working_directory)
        # re-set the config file
        config_run.set_config_file()

        ## create working dir
        if not os.path.isdir(config_run.working_directory):
            os.makedirs(config_run.working_directory)

        ## end date update
        if config_run.end_date not in [None, 'None'] and (config_run.end_date.date < config_run.target_date.date):
            config_run.end_date = None

        ## download type
        if args.download_type not in [None, 'None']:
            config_run.download_type = args.download_type
        elif config_run.download_type in [None, 'None']:
            config_run.download_type = 'steps'

        ## email
        config_run.email = args.email
        if config_run.email is not None:
            config_run.email = config_run.email.split(',')

        if config_run.dnld_errors is not None:
            config_run.dnld_errors = config_run.dnld_errors.split(',')
        elif config_run.dnld_errors == 'None':
            config_run.dnld_errors = None

        config_run.save()

        #########################
        ## checks
        if config_run.end_date in [None, 'None'] and config_run.download_type == 'full':
            print("\nError: you need specify the 'end_date' in download_settings.cfg or '--to' in arguments\n" \
                  "when the download type is 'full'.")
            exit()
        if config_run.email not in [None, 'None'] and config_run.end_date in [None, 'None']:
            print("\nError: you need specify the 'end_date' in download_settings.cfg or '--to' in arguments\n" \
                  "when you want send email when finnish.")
            exit()

        #########################
        ## check the current and end date are equal, not run because the download is already finished
        if config_run.end_date not in [None, 'None'] and (config_run.target_date.date == config_run.end_date.date):
            if config_run.dnld_finished:
                msg = '\nThe download is already finished!\n\nExit'
                print(msg)
                exit()

    if args.make == 'process':
        config_run = ConfigRun(args.working_directory, args.make)
        config_run.load()

        #########################
        ## load and set config_run variables

        # load the download_settings.cfg for get the start_date and end_date
        download_config_run = ConfigRun(os.path.dirname(os.path.dirname(args.working_directory)), 'download')
        download_config_run.load()

        if config_run.source is None and download_config_run.source is not None:
            source = [os.path.basename(config_run.working_directory)]
            if source[0] in download_config_run.source.split(','):
                config_run.source = source
        elif config_run.source is not None:
            config_run.source = [config_run.source]

        # required prev_rundir only for p4_stats for computes statistics
        config_run.prev_rundir = args.prev_rundir

        config_run.start_date = download_config_run.start_date
        config_run.end_date = download_config_run.end_date

    return config_run


## example
#config_run = ConfigRun('/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/')
#config_run.load()
#print config_run.month_to_process
#print config_run.months_made
#### do something
#config_run.months_made = 3
#config_run.save()