#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import os
from copy import deepcopy
from datetime import date

from ATD.lib import ConfigRun, DateATD, dir_date_name, email_download_complete, update_folder_name

def get(args):
    # global_path_to_run = '/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/'
    # global_path_to_run = '/Modelo_Raster/Modelos/Modelos1/Alertas_Temp_Deforest/'

    if args.make == 'download':

        config_run = ConfigRun(args.path)
        config_run.load()

        print vars(config_run)

        #########################
        ## loads all arguments and settings.cfg into config_run

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

        ## current working dir
        if config_run.current_working_dir in [None, 'None']:
            config_run.current_working_dir = dir_date_name(config_run.start_date, config_run.target_date)
        elif config_run.current_working_dir != dir_date_name(config_run.start_date, config_run.target_date):
            msg = '\nWarning: The current working directory not match with the start \n' \
                  'and target date parameters, creating new work directory: ' + config_run.current_working_dir
            print msg
            config_run.current_working_dir = dir_date_name(config_run.start_date, config_run.target_date)

        ## current working dir update
        config_run.abs_path_dir = os.path.abspath(os.path.join(config_run.path_to_run, config_run.current_working_dir))
        if not os.path.isdir(config_run.abs_path_dir):
            msg = '\nWarning: The current working directory {0} not exists, \n' \
                  'start again the download in new work directory {1}: '.format(config_run.current_working_dir,
                                                                                dir_date_name(config_run.start_date,
                                                                                              config_run.start_date))
            print msg
            config_run.target_date = deepcopy(config_run.start_date)
            config_run.current_working_dir = dir_date_name(config_run.start_date, config_run.target_date)
            config_run.abs_path_dir = os.path.abspath(
                os.path.join(config_run.path_to_run, config_run.current_working_dir))
            os.makedirs(config_run.abs_path_dir)

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
            update_folder_name(config_run)
            # move settings into directory
            os.rename(config_run.config_file,
                      os.path.join(config_run.abs_path_dir, os.path.basename(config_run.config_file)))

            # start new instance (restart) and continue
            del config_run
            return get(args)

            ## update the target date
            # config_run.target_date.next()

    if args.make == 'process':
        config_run = ConfigRun(args.folder)
        config_run.load()

        config_run.abs_path_dir = os.path.abspath(config_run.path_to_run)


    # print config_run.current_working_dir
    # print config_run.start_date
    # print vars(config_run)
    # print config_run.end_date


    return config_run
