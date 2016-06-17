#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
from datetime import datetime

from atd.download.dnld_manager import DownloadManager
from atd.lib import fix_zeros_in_datetime, dirs_and_files_in_url

scenes = [
    'h10v07',
    'h10v08',
    'h10v09',
    'h11v07',
    'h11v08',
    'h11v09',
]

def download(config_run, name):
    global scenes

    if name in ['MOD09A1', 'MOD09Q1', 'MYD09A1', 'MYD09Q1']:
        urls_files, errors = mxd09a1_q1(config_run, name)
    if name in ['MOD09GA', 'MOD09GQ', 'MYD09GA', 'MYD09GQ']:
        urls_files, errors = mxd09ga_gq(config_run, name)

    # ==============================================================================
    # check before download files

    # not errors but the list of files is empty
    if len(errors) == 0 and len(urls_files) == 0:
        msg = '\nHave not been reported errors, but there are not\n' \
              'files to download for {0} for the date {1}' \
            .format(name, config_run.target_date.date.strftime('%Y-%m-%d'))

        config_run.dnld_logfile.write(msg + '\n')
        print(msg)
        # report and continue
        errors.append('There are not files to download for {0} for the date {1}'
                      .format(name, config_run.target_date.date.strftime('%Y-%m-%d')))

    # ==============================================================================
    # download files

    for scene in scenes:
        scene_files = [x for x in urls_files if scene in x]
        # create instance of DownloadManager class
        dnld_manager = DownloadManager(num_workers=3)
        dnld_manager.dnld_name = name + '_' + scene
        dnld_manager.dnld_date = config_run.target_date.date.strftime('%Y-%m-%d')
        dnld_manager.dnld_logfile = config_run.dnld_logfile
        dnld_manager.dnld_statusfile = os.path.join(config_run.download_path, name, name + "_status.csv")
        dnld_manager.errors = errors

        # define destination directory
        dest_dir = os.path.join(config_run.download_path, name, scene)

        # download in parallel
        dnld_manager.main(scene_files, dest_dir)

        # TODO: clean before run
        # delete old directory older than 3 days
        # dnld_manager.clean_old_files(2)

        # ==============================================================================
        # print download status when finished

        dnld_manager.download_status()

        del dnld_manager

    # return number of errors, status file
    return DownloadManager.DNLD_ERRORS, os.path.join(config_run.download_path, name, name + "_status.csv")


def mxd09a1_q1(config_run, name):
    """For products modis: MOD09A1, MOD09Q1, MYD09A1, MYD09Q1

    Surface Reflectance 8-Day L3: http://modis.gsfc.nasa.gov/data/dataprod/mod09.php
    """
    global scenes
    # save any error occur
    errors = []
    # ==============================================================================
    # name and datetime

    today = datetime.today()
    dnld_datetime = "{year}{month}{day}" \
        .format(year=today.year, month=fix_zeros_in_datetime(today.month),
                day=fix_zeros_in_datetime(today.day))

    # ==============================================================================
    # defined files to download

    # download from terra
    if config_run.current_source == 'terra':
        url_source = 'http://e4ftl01.cr.usgs.gov/MOLT'
    # download from aqua
    if config_run.current_source == 'aqua':
        url_source = 'http://e4ftl01.cr.usgs.gov/MOLA'

    urls_files = []
    url = '{url_source}/{name}.006/{year}.{month}.{day}/' \
        .format(url_source=url_source, name=name, year=config_run.target_date.date.year,
                month=fix_zeros_in_datetime(config_run.target_date.date.month),
                day=fix_zeros_in_datetime(config_run.target_date.date.day))

    files, status = dirs_and_files_in_url(url)
    if status != 'ok':
        errors.append(status)
    else:
        for scene in scenes:
            for file in files:
                if name in file and scene in file and \
                                "BROWSE" not in file and '.xml' not in file:
                    urls_files.append(url + file)

    return urls_files, errors


def mxd09ga_gq(config_run, name):
    """For products modis: MOD09GA, MOD09GQ, MYD09GA, MYD09GQ

    Surface Reflectance Daily L2G: http://modis.gsfc.nasa.gov/data/dataprod/mod09.php
    """
    global scenes
    # save any error occur
    errors = []
    # ==============================================================================
    # name and datetime

    today = datetime.today()
    dnld_datetime = "{year}{month}{day}" \
        .format(year=today.year, month=fix_zeros_in_datetime(today.month),
                day=fix_zeros_in_datetime(today.day))

    # ==============================================================================
    # defined files to download

    # download from terra
    if config_run.current_source == 'terra':
        url_source = 'http://e4ftl01.cr.usgs.gov/MOLT'
    # download from aqua
    if config_run.current_source == 'aqua':
        url_source = 'http://e4ftl01.cr.usgs.gov/MOLA'

    urls_files = []
    url = '{url_source}/{name}.006/{year}.{month}.{day}/' \
        .format(url_source=url_source, name=name, year=config_run.target_date.date.year,
                month=fix_zeros_in_datetime(config_run.target_date.date.month),
                day=fix_zeros_in_datetime(config_run.target_date.date.day))

    files, status = dirs_and_files_in_url(url)
    if status != 'ok':
        errors.append(status)
    else:
        for scene in scenes:
            for file in files:
                if name in file and scene in file and \
                                "BROWSE" not in file and '.xml' not in file:
                    urls_files.append(url + file)

    return urls_files, errors
