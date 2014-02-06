#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import __init__
import os
import sys
from download import DownloadManager
from lib import enumeration, fix_zeros_in_datetime
from datetime import datetime

#==============================================================================
# name and datetime

name = 'gfs'
hour = sys.argv[1]
dnld_name = '{0}-{1}'.format(name, fix_zeros_in_datetime(hour))

today = datetime.today()
dnld_date = "{year}{month}{day}" \
    .format(year=today.year, month=fix_zeros_in_datetime(today.month),
            day=fix_zeros_in_datetime(today.day))
dnld_hour = fix_zeros_in_datetime(hour)
dnld_datetime = dnld_date+dnld_hour

#==============================================================================
# defined files to download

urls_files = []
for enum in enumeration(0, 12, 6):
    url = 'ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{dnld_datetime}/gfs.t12z.pgrbf{numfile}.2p5deg.grib2'\
        .format(dnld_datetime=dnld_datetime, numfile=enum)
    urls_files.append(url)

#==============================================================================
# download files

# create instance of DownloadManager class
dnld_manager = DownloadManager(num_workers=2)
dnld_manager.dnld_name = dnld_name
dnld_manager.dnld_datetime = dnld_datetime

# defined destination directory
dest_dir = os.path.join("/home/xavier/", name, dnld_date, dnld_hour)

# download in parallel
dnld_manager.download_files(urls_files, dest_dir)

# delete old directory
dnld_manager.clean_old_files(3)

#==============================================================================
# print download status when finished

dnld_manager.download_status()

