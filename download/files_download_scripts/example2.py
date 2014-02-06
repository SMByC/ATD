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

name = 'obs'
hour = sys.argv[1]
dnld_name = '{0}_{1}'.format(name, fix_zeros_in_datetime(hour))

today = datetime.today()
dnld_date = "{year}{month}{day}"\
    .format(year=today.year, month=fix_zeros_in_datetime(today.month),
            day=fix_zeros_in_datetime(today.day))
dnld_hour = fix_zeros_in_datetime(hour)
dnld_datetime = dnld_date+dnld_hour

#==============================================================================
# defined files to download

urls_files = []
url = 'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{dnld_datetime}/gfs.t{hour}z.prepbufr.unblok.nr'\
    .format(dnld_datetime=dnld_datetime, hour=hour)
urls_files.append(url)
url = 'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{dnld_datetime}/gfs.t{hour}z.gpsro.tm00.bufr_d'\
    .format(dnld_datetime=dnld_datetime, hour=hour)
urls_files.append(url)

#==============================================================================
# download files

# create instance of DownloadManager class
dnld_manager = DownloadManager(num_workers=4)
dnld_manager.dnld_name = dnld_name
dnld_manager.dnld_datetime = dnld_datetime

# defined destination directory
dest_dir = os.path.join(".", name, dnld_date, dnld_hour)

# download in parallel
dnld_manager.download_files(urls_files, dest_dir)

# delete old directory older than 3 days
dnld_manager.clean_old_files(3)

#==============================================================================
# print download status when finished

dnld_manager.download_status()

