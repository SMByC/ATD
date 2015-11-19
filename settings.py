#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import os
from copy import deepcopy
from datetime import date
import ConfigParser
from dateutil.parser import parse


from ATD.lib import DateATD, dir_date_name, email_download_complete, update_working_directory


###############################################################################


class ConfigRun():
    list_of_process = ['p1_tiseg', 'p2_mrt', 'p3_nodata', 'p4_stats', 'p5_nodata', 'p6_mosaic', 'p7_layerstack']

    def __init__(self, working_directory):
        ## [General]
        self.source = None
        self.start_date = None
        self.target_date = None
        self.end_date = None
        ## [Download]
        self.download_type = None
        self.dnld_logfile = None
        self.dnld_errors = None
        self.dnld_finished = False
        ## [Process]

        ## variables that not save into settings
        self.working_directory = os.path.abspath(working_directory)
        self.config_file = os.path.join(self.working_directory, 'settings.cfg')
        self.email = None
        self.download_path = None  # complete path to download (working_directory + 'p0_download')

        ## init process list
        for p in ConfigRun.list_of_process:
            exec ('self.' + p + ' = None')
        # create the dictionary access process
        self.process_ = {'p1_tiseg': self.p1_tiseg, 'p2_mrt': self.p2_mrt,
                         'p3_nodata': self.p3_nodata, 'p4_stats': self.p4_stats,
                         'p5_nodata': self.p5_nodata, 'p6_mosaic': self.p6_mosaic,
                         'p7_layerstack': self.p7_layerstack}

    def create(self, source=None, rundir=None, start_date=None, target_date=None,
               end_date=None, download_type='steps', dnld_errors=None, dnld_finished=False):
        #### values by default
        _months_to_run = 6  # meses a correr (periodo)

        self.source = source
        self.rundir = rundir

        if start_date is not None:
            self.start_date = parse(start_date).date()
        else:
            self.start_date = None

        if target_date is not None:
            self.target_date = parse(target_date).date()
        else:
            self.target_date = deepcopy(self.start_date)

        if end_date is not None:
            self.end_date = parse(end_date).date()
        else:
            self.end_date = None

        self.download_type = download_type
        self.dnld_errors = dnld_errors
        self.dnld_finished = dnld_finished

        self.save()

    def load(self):
        config = ConfigParser.RawConfigParser()
        if not os.path.isfile(self.config_file):
            self.create()
            return
        config.read(self.config_file)
        ## [General]
        self.source = config.get('General', 'source')
        self.start_date = config.get('General', 'start_date')
        self.target_date = config.get('General', 'target_date')
        self.end_date = config.get('General', 'end_date')
        ## [Download]
        self.download_type = config.get('Download', 'download_type')
        self.dnld_errors = config.get('Download', 'dnld_errors')
        self.dnld_finished = config.get('Download', 'dnld_finished')
        ## [Process]
        for p in ConfigRun.list_of_process:
            exec ("self." + p + " = config.get('Process', '" + p + "')")
        # create the dictionary access process
        self.process_ = {'p1_tiseg': self.p1_tiseg, 'p2_mrt': self.p2_mrt,
                         'p3_nodata': self.p3_nodata, 'p4_stats': self.p4_stats,
                         'p5_nodata': self.p5_nodata, 'p6_mosaic': self.p6_mosaic,
                         'p7_layerstack': self.p7_layerstack}

    def save(self):
        config = ConfigParser.RawConfigParser()
        config.add_section('General')
        config.set('General', 'source',
                   ','.join(self.source) if self.source not in [None, 'None'] else 'None')
        config.set('General', 'start_date', self.start_date)
        config.set('General', 'target_date', self.target_date)
        config.set('General', 'end_date', self.end_date)
        config.add_section('Download')
        config.set('Download', 'download_type', self.download_type)
        config.set('Download', 'dnld_errors',
                   ','.join(self.dnld_errors) if self.dnld_errors not in [None, 'None'] else 'None')
        config.set('Download', 'dnld_finished', self.dnld_finished)
        config.add_section('Process')
        for p in ConfigRun.list_of_process:
            exec ("config.set('Process', '" + p + "', self." + p + ")")

        # Writing our configuration file to 'example.cfg'
        with open(self.config_file, 'wb') as configfile:
            config.write(configfile)

###############################################################################


def get(args):
    # global_path_to_run = '/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/'
    # global_path_to_run = '/Modelo_Raster/Modelos/Modelos1/Alertas_Temp_Deforest/'

    if args.make == 'download':

        config_run = ConfigRun(args.working_directory)
        config_run.load()

        #########################
        ## loads all arguments and settings.cfg into config_run

        ## source
        if args.source not in [None, 'None']:
            args.source = args.source.split(',')

        if config_run.source in [None, 'None']:
            config_run.source = args.source

        if args.source not in [['terra'], ['aqua'], ['terra','aqua'], ['aqua','terra']]:
            print "\nError: the source in argument is not recognized, this should be\n" \
                  "'terra', 'aqua' or 'terra,aqua'."
            exit()

        if args.source not in [None, 'None']:
            if args.source != config_run.source:
                print "\nError: the source in settings.cfg and in arguments are different\n" \
                  "if you want run other source, finished/delete the other source before run this."
                exit()

        ## start date
        if args.from_date is not None:
            config_run.start_date = DateATD(args.from_date, 'start')
        elif config_run.start_date in [None, 'None']:
            today = date.today()
            config_run.start_date = DateATD(today.strftime('%Y-%m-%d'), 'start')
        else:
            config_run.start_date = DateATD(config_run.start_date, 'start')

        ## end date
        if args.to_date is not None:
            config_run.end_date = DateATD(args.to_date, 'end')
        elif isinstance(config_run.end_date, str) and not config_run.end_date in [None, 'None']:
            config_run.end_date = DateATD(config_run.end_date, 'end')

        ## target date
        if config_run.target_date in [None, 'None']:
            config_run.target_date = deepcopy(config_run.start_date)
        else:
            config_run.target_date = DateATD(config_run.target_date)
            # test if target date are between the start and end date
            if not (config_run.start_date.date <= config_run.target_date.date):
                msg = '\nWarning: The start date is bigger than target date, changing\n' \
                      'the target date to start date'
                print msg
                config_run.target_date = deepcopy(config_run.start_date)

        ## fix name the working directory
        if config_run.working_directory == os.getcwd():
            config_run.working_directory = os.path.abspath(dir_date_name(config_run.start_date, config_run.target_date))
            msg = '\nWarning: The working directory is empty, not set in arguments, \n' \
                  'start new empty working directory base on dates of run:\n\t' + \
                  config_run.working_directory
            print msg
        elif os.path.basename(config_run.working_directory) != dir_date_name(config_run.start_date, config_run.target_date):
            config_run.working_directory = os.path.abspath(dir_date_name(config_run.start_date, config_run.target_date))
            msg = '\nWarning: The current working directory not match with the start \n' \
                  'and target date parameters, setting the new working directory to:\n\t' + \
                  config_run.working_directory
            print msg

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
            print "\nError: you need specify the 'end_date' in settings.cfg or '--to' in arguments\n" \
                  "when the download type is 'full'."
            exit()
        if config_run.email not in [None, 'None'] and config_run.end_date in [None, 'None']:
            print "\nError: you need specify the 'end_date' in settings.cfg or '--to' in arguments\n" \
                  "when you want send email when finnish."
            exit()

        #########################
        ## check the current and end date are equal, finished criteria
        if config_run.end_date not in [None, 'None'] and (config_run.target_date.date == config_run.end_date.date):
            print config_run.end_date.date
            msg = '\nWarning: the target date is equal or bigger than the end date.\nexit'
            print msg

            # send mail
            if config_run.email is not None:
                email_download_complete(config_run)
            # rename folder
            update_working_directory(config_run)

            ## move settings into directory  TODO settings
            #os.rename(config_run.config_file,
            #          os.path.join(config_run.abs_path_dir, os.path.basename(config_run.config_file)))

            # start new instance (restart) and continue
            del config_run
            return get(args)

            ## update the target date
            # config_run.target_date.next()

    if args.make == 'process':
        config_run = ConfigRun(args.working_directory)
        config_run.load()

    return config_run


## example
#config_run = ConfigRun('/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/')
#config_run.load()
#print config_run.month_to_process
#print config_run.months_made
#### do something
#config_run.months_made = 3
#config_run.save()