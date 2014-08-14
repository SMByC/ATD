#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
from datetime import date, datetime

from lib import email_download_complete, update_folder_name, datetime_format
from download.files_download_scripts import modis

def run(config_run):
    ######################################## pre download ########################################

    # prepare directory to download
    config_run.download_path = os.path.join(config_run.abs_path_dir, 'p0_download')
    if not os.path.isdir(config_run.download_path):
        os.makedirs(config_run.download_path)

    # set the log file for download
    config_run.dnld_logfile = open(os.path.join(config_run.abs_path_dir, 'p0_download','download.log'), 'a')

    # init log of download
    msg = '\n\n########### START LOG FOR: target:'+config_run.target_date.date.strftime('%Y-%m-%d')+' in dir:'+config_run.current_working_dir+' - ('+datetime_format(datetime.today())+') ###########'
    config_run.dnld_logfile.write(msg+'\n')
    print msg

    # check if the download date is greater than the target date
    today = date.today()
    if config_run.target_date.date >= today:
        msg = '\nThe target date for download files {0} is greater than current date {1}'\
            .format(config_run.target_date.date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d'))
        config_run.dnld_logfile.write(msg+'\n')
        print msg
        exit()

    dnld_errors = []

    ########################################## download ##########################################

    def do_download():
        # print message of start download
        msg = '\n#### Download target {0} (in dir: {1}) - ({2})'.format(config_run.target_date.date.strftime('%Y-%m-%d'),
                                                            config_run.current_working_dir, datetime_format(datetime.today()))
        config_run.dnld_logfile.write(msg+'\n')
        print msg

        # download
        dnld_errors_A1, status_file_A1 = modis.download(config_run, 'MOD09A1')
        dnld_errors_Q1, status_file_Q1 = modis.download(config_run, 'MOD09Q1')

        msg_error = None
        ## check errors from download
        if dnld_errors_A1 != 0 or dnld_errors_Q1 != 0:
            dnld_errors.append(config_run.target_date.date)
            msg_error = 'with'
        elif config_run.dnld_errors is not None:
            # without error, then delete date in errors list if exist
            if config_run.target_date.date.strftime('%Y-%m-%d') in config_run.dnld_errors:
                config_run.dnld_errors.remove(config_run.target_date.date.strftime('%Y-%m-%d'))
            msg_error = 'without'

        # print message of state of download
        msg = '\n#### Download finished for {0} {1} errors - ({2})'.format(config_run.target_date.date.strftime('%Y-%m-%d'),
                                                                         msg_error, datetime_format(datetime.today()))
        config_run.dnld_logfile.write(msg+'\n')
        print msg

        # update the target date
        config_run.target_date.next()
        # rename folder
        update_folder_name(config_run)
        # update/save config file
        config_run.save()


    if config_run.download_type == 'steps':
        do_download()

    if config_run.download_type == 'full':
        while True:
            do_download()

            if config_run.target_date.date >= config_run.end_date.date:
                break

    ######################################## post download #######################################

    # errors
    if dnld_errors:
        dnld_errors = [x.strftime('%Y-%m-%d') for x in dnld_errors]
        if config_run.dnld_errors[0] == 'None':
            config_run.dnld_errors = dnld_errors
        else:
            config_run.dnld_errors = config_run.dnld_errors + dnld_errors
        config_run.save()

    ## check the current and end date are equal, finished criteria
    if config_run.end_date not in [None, 'None'] and (config_run.target_date.date >= config_run.end_date.date):
        print config_run.end_date.date
        msg = '\nThe target date {0} is equal or bigger than\n' \
              'the end date {1} in settings.cfg file.\n' \
              'Download completed!\n'.format(config_run.target_date.date.strftime('%Y-%m-%d'),
                                           config_run.end_date.date.strftime('%Y-%m-%d'))
        config_run.dnld_logfile.write(msg+'\n')
        print msg
        # send mail
        if config_run.email is not None:
            email_download_complete(config_run)

        # update/save config file
        config_run.dnld_finished = True
        config_run.save()

        # move settings into directory
        os.rename(config_run.config_file,
                  os.path.join(config_run.abs_path_dir, os.path.basename(config_run.config_file)))

    # close log file
    config_run.dnld_logfile.close()