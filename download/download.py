#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import __init__
import csv
import os
import shutil
from random import randint
from threading import Thread
from Queue import Queue
from subprocess import call
from datetime import datetime
from time import sleep
from dateutil.relativedelta import relativedelta

from ATD.lib import datetime_format, cksum


class File:

    name = None
    url = None
    path = None
    file_check = None

    def __init__(self, DnldMan, url):
        self.url = url
        self.name = os.path.basename(url)
        # is destination was defined
        if DnldMan.DEST:
            self.path = os.path.join(DnldMan.DEST, self.name)
        else: # save in the same directory of run
            self.path = self.name

    def getsize(self):
        if os.path.isfile(self.path):
            return str(round(os.path.getsize(self.path)/1000000.0,1)) + ' MB'
        else:
            return '--'

    def check(self, DnldMan):
        '''
        check files with md5sum, cksum,...

        return:
            0, 'cksum checked'
            1, 'not check files'
            2, 'impossible checked'
            3, 'error, the cksum is different'
        '''

        if not DnldMan.check_files:
            return 1, 'not check files'

        #### MODIS files #####
        file_with_cksum = self.url + '.xml'
        wget_status =  call(DnldMan.wget_cmd + [file_with_cksum], shell=False)
        if wget_status == 0:
            file_cksum = open(self.path + '.xml', 'rU')
            lines_cksum = file_cksum.readlines()
            file_cksum.close()

            for line in lines_cksum:
                if line.strip().startswith('<Checksum>'):
                    cksum_from_file = line.replace('<Checksum>','').replace('</Checksum>','').strip()
                    cksum_from_file = int(cksum_from_file)
                    if cksum_from_file == cksum(self.path):
                        return 0, 'cksum checked'
                    else:
                        return 3, 'error, the cksum is different'
        else:
            return 2, 'impossible checked'



class DownloadManager:
    """Download manager class for download in parallel using queues, treads and wget
    """

    # attempts for download it again if has any error
    NUM_ATTEMPT = 4
    # wait time for the next attempt (seconds)
    WAIT_TIME_ATTEMPT = 300
    # attempts for daemond download
    NUM_ATTEMPT_DAEMON = 20
    # wait time for the daemon sleep for check if exist files for start download (seconds)
    WAIT_TIME_DAEMON = 300

    # Default wget options to use for downloading each file
    WGET = ["wget", "-q", "-nd", "-np", "-c"]  # "r" for recursive
    # path to save files, if is None then download in the same run directory
    DEST = None

    # save if there are any error
    DNLD_ERRORS = 0

    ## download with urllib2  ----------------------------
    # import shutil
    # import urllib2
    # from contextlib import closing
    #
    # url="ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.2013122518/gfs.t18z.pgrb2bf117"
    # url="http://s.download.nvidia.com/XFree86/Linux-x86-ARM/331.38/NVIDIA-Linux-armv7l-gnueabihf-331.38.run"
    #
    # with closing(urllib2.urlopen(url)) as r:
    #     with open('file', 'wb') as f:
    #         shutil.copyfileobj(r, f)

    def __init__(self, num_workers):
        self.Q = Queue()
        self.check_files = True
        self.logs_path = None
        self.logs_name = None
        for i in range(num_workers):
            t = Thread(target=self.worker)
            t.setDaemon(True)
            t.start()

    def main(self, urls_files, DEST=False):
        Q = Queue() # Create a second queue so the worker
        self.DEST = DEST
        self.start_dnld_datetime = datetime.today()

        # make destination directory
        if self.DEST:
            if not os.path.isdir(self.DEST):
                os.makedirs(self.DEST)

        # logs name and path
        if self.DEST and self.logs_path is None:
            self.logs_path = self.DEST
        if self.logs_name is None:
            self.logs_name = self.dnld_name

        # save log
        self.log = open(os.path.join(self.logs_path, self.logs_name+'_'+self.dnld_date+'.log'), 'a')

        self.log.write('\n########### START LOG FOR: '+self.dnld_name+' - '+self.dnld_date+' - ('+datetime_format(datetime.today())+')\n')

        # is destination was defined
        if self.DEST:
            self.wget_cmd = self.WGET + ["-P", self.DEST]
        else: # save in the same directory of run
            self.wget_cmd = self.WGET

        # start daemon request for check available files
        self.daemon_request(urls_files)

        # threads can send the data back again
        for url in urls_files:
            # create file instance
            new_file = File(self, url)
            # Add the URLs in `urls` to be downloaded asynchronously
            self.Q.put((new_file, Q))

        dnld_status = []
        for i in range(len(urls_files)):
            # Get the data as it arrives, raising
            # any exceptions if they occur
            status, file = Q.get()

            dnld_status.append([file.name, file.getsize(), status])

        self.dnld_status = dnld_status
        # save errors produced in the download
        for status in self.dnld_status:
            if status[2] == 'ERROR':
                self.errors.append('error downloading file '+status[0])

        DownloadManager.DNLD_ERRORS = len(self.errors)

        # close log file
        self.log.close()

    def worker(self):
        while True:
            file, Q = self.Q.get()

            file_download_ok = False

            for attempt in range(self.NUM_ATTEMPT):
                self.log.write('download started:  ' + file.url + ' (' + datetime_format(datetime.today())+')\n')
                self.log.flush()
                # download with wget
                wget_status =  call(self.wget_cmd + [file.url], shell=False)
                # TODO: check time for wget process
                # check wget_status
                if wget_status == 0:
                    # check cksum
                    check_status, check_msg = file.check(self)
                    if check_status in [0,1]:
                        self.log.write('download finished: ' + file.url + ' (' + datetime_format(datetime.today())+') - '+check_msg+'\n')
                        self.log.flush()
                        file_download_ok = True
                        Q.put(('OK ('+check_msg+')', file))
                        break
                    else:
                        self.log.write('error downloading, attempt '+str(attempt)+', '+check_msg+', try again: ' +file.url+' ('+datetime_format(datetime.today())+')\n')
                        sleep(self.WAIT_TIME_ATTEMPT)
                else:
                    # if wget_status != 0 is due a some error
                    self.log.write('error downloading, attempt '+str(attempt)+', '+file.url+' ('+datetime_format(datetime.today())+')\n')
                    self.log.flush()
                    sleep(self.WAIT_TIME_ATTEMPT)

            if not file_download_ok:
                Q.put(('ERROR', file))

    def daemon_request(self, urls_files):
        self.log.write('Errors reported before download:\n')
        if len(urls_files) == 0 or len(self.errors) != 0:
            for error in self.errors:
                self.log.write('   '+error+'\n')
                self.log.flush()
            if len(urls_files) == 0:
                self.log.write('   no files to download!!, '+ datetime_format(datetime.today()) + '\n')
                self.log.flush()
                return False
        else:
            self.log.write('   no error reported, '+ datetime_format(datetime.today()) + '\n')
            self.log.flush()
        self.log.write('\n')

        import urllib2

        def file_exists(url):
            request = urllib2.Request(url)
            request.get_method = lambda : 'HEAD'
            try:
                response = urllib2.urlopen(request)
                return True
            except:
                return False

        for attempt in range(self.NUM_ATTEMPT_DAEMON):
            url_file_to_test = urls_files[randint(0,len(urls_files)-1)] # take a random file
            if file_exists(url_file_to_test):
                self.log.write('ready to download ' + datetime_format(datetime.today()) + '\n')
                self.log.flush()
                return True
            else:
                self.log.write('waiting for available files, attempt ' + str(attempt) + ', '+ datetime_format(datetime.today()) + '\n')
                self.log.flush()
                sleep(self.WAIT_TIME_DAEMON)

        # impossible request files to download... exit
        self.log.write('maximum of attempt for daemon, impossible request files to download, exiting ' + datetime_format(datetime.today()) + '\n')
        self.log.flush()
        #TODO: sendmail error
        exit()

    def download_status(self):

        file_dnld_status = os.path.join(self.logs_path, self.logs_name+'_'+self.dnld_date+"_status.csv")

        open_dnld_status = open(file_dnld_status, 'a')
        csv_dnld_status = csv.writer(open_dnld_status, delimiter=';')

        self.end_dnld_datetime = datetime.today()

        csv_dnld_status.writerow([])
        csv_dnld_status.writerow(['########### START LOG STATUS FOR: '+self.dnld_name+' - '+self.dnld_date])
        csv_dnld_status.writerow([self.dnld_name, self.dnld_date])

        csv_dnld_status.writerow(['started: ' + datetime_format(self.start_dnld_datetime),
                                  'finished: ' + datetime_format(self.end_dnld_datetime)])

        for status in self.dnld_status:
            csv_dnld_status.writerow(status)

        # report errors
        csv_dnld_status.writerow([])
        if len(self.errors) != 0:
            csv_dnld_status.writerow(['Errors:'])
            for error in self.errors:
                csv_dnld_status.writerow([error])
        else:
            csv_dnld_status.writerow(['No errors reported'])

        # close log file
        open_dnld_status.close()

    def clean_old_files(self, days_to_storage):

        # save log
        if self.DEST:
            self.log = open(os.path.join(self.DEST, self.dnld_name + '.log'), 'a')
        else:
            self.log = open(self.dnld_name + '.log', 'a')

        base_path = os.path.dirname(os.path.dirname(self.DEST))

        # get all directories in base_path in top level
        dirs = os.walk(base_path).next()[1]

        datetime_limit = datetime.today() + relativedelta(days=-days_to_storage)

        for _dir in dirs:
            try:
                year = int(_dir[0:4])
                month = int(_dir[4:6])
                day = int(_dir[6:8])
                #hour = int(_dir[8:10])
                datetime_dir = datetime(year, month, day)
            except:
                self.log.write('unknown format directory (for clean old files): ' + os.path.join(base_path, _dir) + '\n')

            if datetime_dir < datetime_limit:
                # delete directory
                shutil.rmtree(os.path.join(base_path, _dir), ignore_errors=True)
                self.log.write('cleaning old directory: ' + os.path.join(base_path, _dir) + '\n')

        # close log file
        self.log.close()
