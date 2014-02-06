#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import __init__
import os
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from ATD.download.download import DownloadManager
from ATD.lib import enumeration, fix_zeros_in_datetime, send_mail, dirs_and_files_in_url


scenes = [
    'h10v07',
    'h10v08',
    'h10v09',
    'h11v07',
    'h11v08',
    'h11v09',
]

def get_all_start_days_of_month(year, month):
    reference_date = date(2014,01,01)

    if reference_date.year > year:
        raise Exception("The date is bigger than reference date")

    tmp_date = reference_date
    while year != tmp_date.year:
        tmp_date += relativedelta(days=8)
    while month != tmp_date.month:
        tmp_date += relativedelta(days=8)

    list_days = []
    while month == tmp_date.month:
        list_days.append(tmp_date.day)
        tmp_date += relativedelta(days=8)

    return list_days


def download(name, dnld_year, dnld_month):

    #==============================================================================
    # name and datetime

    today = datetime.today()
    dnld_datetime = "{year}{month}{day}"\
        .format(year=today.year, month=fix_zeros_in_datetime(today.month),
                day=fix_zeros_in_datetime(today.day))

    dnld_date = "{0}-{1}".format(dnld_year, dnld_month)

    #==============================================================================
    # defined files to download

    urls_files = []
    days = get_all_start_days_of_month(dnld_year, dnld_month)
    for day in days:
        url = 'http://e4ftl01.cr.usgs.gov/MOLT/{name}.005/{year}.{month}.{day}/'\
            .format(name=name, year=dnld_year, month=fix_zeros_in_datetime(dnld_month), day=fix_zeros_in_datetime(day))

        dirs, files = dirs_and_files_in_url(url)

        for scene in scenes:
            for file in files:
                if name in file and scene in file and \
                'BROWSE' not in file and '.xml' not in file:
                    urls_files.append(url+file)



    urls_files = [
        'http://e4ftl01.cr.usgs.gov/MOLT/MOD09A1.005/2000.04.06/MOD09A1.A2000097.h09v02.005.2008198085600.hdf',
        'http://e4ftl01.cr.usgs.gov/MOLT/MOD09A1.005/2000.04.14/MOD09A1.A2000105.h09v02.005.2006266185153.hdf',
    ]


    #==============================================================================
    # download files

    # create instance of DownloadManager class
    dnld_manager = DownloadManager(num_workers=1)
    dnld_manager.dnld_name = name
    dnld_manager.dnld_date = dnld_date

    # defined destination directory
    dest_dir = os.path.join("/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp", name)

    # download in parallel
    dnld_manager.main(urls_files, dest_dir)

    # TODO: clean before run
    # delete old directory older than 3 days
    #dnld_manager.clean_old_files(2)

    #==============================================================================
    # print download status when finished

    dnld_manager.download_status()


download('MOD09A1', 2014, 1)