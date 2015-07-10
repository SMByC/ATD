#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014-2015, SMBYC - IDEAM
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import os
from datetime import datetime

from download.dnld_manager import DownloadManager
from lib import fix_zeros_in_datetime, dirs_and_files_in_url, get_all_start_n_days_of_month


scenes = [
    'h10v07',
    'h10v08',
    'h10v09',
    'h11v07',
    'h11v08',
    'h11v09',
]


def download(config_run, name):
    # save any error occur
    errors = []

    #==============================================================================
    # name and datetime

    today = datetime.today()
    dnld_datetime = "{year}{month}{day}"\
        .format(year=today.year, month=fix_zeros_in_datetime(today.month),
                day=fix_zeros_in_datetime(today.day))

    #==============================================================================
    # defined files to download

    ### test
    # urls_files = [
    #     'http://e4ftl01.cr.usgs.gov/MOLT/MOD09A1.005/2000.04.06/MOD09A1.A2000097.h09v02.005.2008198085600.hdf',
    #     'http://e4ftl01.cr.usgs.gov/MOLT/MOD09A1.005/2000.04.14/MOD09A1.A2000105.h09v02.005.2006266185153.hdf',
    # ]
    # dnld_manager = DownloadManager(num_workers=3)
    # dnld_manager.dnld_name = name
    # dnld_manager.dnld_date = dnld_date
    # #dnld_manager.logs_path = path
    # dnld_manager.errors = errors
    # dest_dir = os.path.join(path, name)
    # dnld_manager.main(urls_files, dest_dir)
    # dnld_manager.download_status()
    #del dnld_manager
    #return DownloadManager.DNLD_ERRORS, os.path.join(dest_dir, name+'_'+dnld_date+"_status.csv")
    ### test

    urls_files = []
    url = 'http://e4ftl01.cr.usgs.gov/MOLT/{name}.005/{year}.{month}.{day}/'\
        .format(name=name, year=config_run.target_date.date.year,
                month=fix_zeros_in_datetime(config_run.target_date.date.month),
                day=fix_zeros_in_datetime(config_run.target_date.date.day))

    dirs, files, status = dirs_and_files_in_url(url)
    if status != 'ok':
        errors.append(status)
    else:
        for scene in scenes:
            for file in files:
                if name in file and scene in file and \
                'BROWSE' not in file and '.xml' not in file:
                    urls_files.append(url+file)


    #==============================================================================
    # check before download files

    # not errors but the list of files is empty
    if len(errors) == 0 and len(urls_files) == 0:
        msg = '\nHave not been reported errors, but there are not\n' \
              'files to download for {0} for the date {1}'\
            .format(name, config_run.target_date.date.strftime('%Y-%m-%d'))

        config_run.dnld_logfile.write(msg+'\n')
        print msg
        exit()

    #==============================================================================
    # download files

    for scene in scenes:
        scene_files = [x for x in urls_files if scene in x]
        # create instance of DownloadManager class
        dnld_manager = DownloadManager(num_workers=3)
        dnld_manager.dnld_name = name+'_'+scene
        dnld_manager.dnld_date = config_run.target_date.date.strftime('%Y-%m-%d')
        dnld_manager.dnld_logfile = config_run.dnld_logfile
        dnld_manager.dnld_statusfile = os.path.join(config_run.download_path, name, name+"_status.csv")
        dnld_manager.errors = errors

        # define destination directory
        dest_dir = os.path.join(config_run.download_path, name, scene)

        # download in parallel
        dnld_manager.main(scene_files, dest_dir)

        # TODO: clean before run
        # delete old directory older than 3 days
        #dnld_manager.clean_old_files(2)

        #==============================================================================
        # print download status when finished

        dnld_manager.download_status()

        del dnld_manager

    # return number of errors, status file
    return DownloadManager.DNLD_ERRORS, os.path.join(config_run.download_path, name, name+"_status.csv")

#download('MOD09A1', "/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp", 2014, 1)