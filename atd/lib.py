#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import urllib.request, urllib.parse, urllib.error

try:
    from osgeo import gdal
except ImportError:
    import gdal
from datetime import date, datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


def datetime_format(dt):
    return dt.strftime("%Y/%m/%d %H:%M")


def fix_zeros_in_datetime(dt):
    return '0' + str(dt) if len(str(dt)) < 2 else str(dt)


def enumeration(start, stop, step):
    """Return enumeration string values, e.g:

    (0,18,3) -> ['00', '03', '06', '09', '12', '15', '18']
    """
    enum = list(range(start, stop + 1, step))
    enum = ['0' + str(i) if len(str(i)) < 2 else str(i) for i in enum]
    return enum


###############################################################################

def send_mail(sender, receiver, subject, body, files_attached=None):
    import smtplib
    import base64
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart

    # Create a text/plain message
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    # This is the textual part:
    part = MIMEText(body, _charset="utf-8")
    msg.attach(part)

    # This is the binary part(The Attachment) if is not None:
    if files_attached is not None and len(files_attached) != 0:
        for file_attached in files_attached:
            part = MIMEApplication(open(file_attached, "rb").read())
            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(file_attached))
            msg.attach(part)

    # Send the message via our own SMTP server
    server = smtplib.SMTP("mail.ideam.gov.co", 587)
    # Next, log in to the server
    server.login(os.environ.get('ideam_mail_user'), os.environ.get('ideam_mail_pass'))
    server.sendmail(msg["From"], msg["To"].split(","), msg.as_string())
    server.quit()


## example:
# send_mail('xcorredorl@ideam.gov.co', 'xavier.corredor.llano@gmail.com', 'test subject', 'bodyy test\nnew lineee', 'howto_config.txt')

def email_download_complete(config_run, files_attached=[]):
    mail_subject = "Reporte de la descarga de Aler.Temp.Defor. para {0}-{1} ({2}/{3})".format(config_run.year_to_run,
                                                                                              config_run.month_to_process,
                                                                                              config_run.months_made,
                                                                                              config_run.months_to_run)
    mail_body = \
        '\n{0}\n\nEste es el reporte automático de la descarga de las\n' \
        'Alertas Tempranas de Deforestación\n\n' \
        'Archivos modis de {1} para el {2}-{3}\n\n' \
        'Realizados {4} mes(es) de {5}\n\n' \
        'La ruta de almacenamiento de los resultados:\n' \
        '   (SAN): {6}\n'.format(datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                                 ','.join(config_run.source).upper(),
                                 config_run.year_to_run,
                                 config_run.month_to_process,
                                 config_run.months_made,
                                 config_run.months_to_run,
                                 config_run.working_directory)

    mail_body += '\nLa descarga se ha completado!.\n'

    if config_run.dnld_errors[0] == 'None':
        mail_body += '\nLos log de descarga no reportaron problemas\n'
    else:
        mail_body += '\nEl log de descarga reporto algun(os) problema(s)' \
                     'para las siguientes fechas: {0}\n'.format(','.join(config_run.dnld_errors))

    if len(files_attached) != 0:
        mail_body += '\nAdjunto se envían los logs del reporte de descarga.\n'

    send_mail('xcorredorl@ideam.gov.co',
              config_run.email,
              mail_subject,
              mail_body,
              files_attached)


###############################################################################

def cksum(file_to_check):  # TODO replace
    """This module implements the cksum command found in most UNIXes in pure
    python.

    The constants and routine are cribbed from the POSIX man page
    """

    crctab = [0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc,
              0x17c56b6b, 0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f,
              0x2f8ad6d6, 0x2b4bcb61, 0x350c9b64, 0x31cd86d3, 0x3c8ea00a,
              0x384fbdbd, 0x4c11db70, 0x48d0c6c7, 0x4593e01e, 0x4152fda9,
              0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75, 0x6a1936c8,
              0x6ed82b7f, 0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3,
              0x709f7b7a, 0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e,
              0x95609039, 0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5,
              0xbe2b5b58, 0xbaea46ef, 0xb7a96036, 0xb3687d81, 0xad2f2d84,
              0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d, 0xd4326d90, 0xd0f37027,
              0xddb056fe, 0xd9714b49, 0xc7361b4c, 0xc3f706fb, 0xceb42022,
              0xca753d95, 0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
              0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077,
              0x30476dc0, 0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c,
              0x2e003dc5, 0x2ac12072, 0x128e9dcf, 0x164f8078, 0x1b0ca6a1,
              0x1fcdbb16, 0x018aeb13, 0x054bf6a4, 0x0808d07d, 0x0cc9cdca,
              0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde, 0x6b93dddb,
              0x6f52c06c, 0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08,
              0x571d7dd1, 0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d,
              0x40d816ba, 0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e,
              0xbfa1b04b, 0xbb60adfc, 0xb6238b25, 0xb2e29692, 0x8aad2b2f,
              0x8e6c3698, 0x832f1041, 0x87ee0df6, 0x99a95df3, 0x9d684044,
              0x902b669d, 0x94ea7b2a, 0xe0b41de7, 0xe4750050, 0xe9362689,
              0xedf73b3e, 0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
              0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683,
              0xd1799b34, 0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59,
              0x608edb80, 0x644fc637, 0x7a089632, 0x7ec98b85, 0x738aad5c,
              0x774bb0eb, 0x4f040d56, 0x4bc510e1, 0x46863638, 0x42472b8f,
              0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53, 0x251d3b9e,
              0x21dc2629, 0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5,
              0x3f9b762c, 0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48,
              0x0e56f0ff, 0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623,
              0xf12f560e, 0xf5ee4bb9, 0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2,
              0xe6ea3d65, 0xeba91bbc, 0xef68060b, 0xd727bbb6, 0xd3e6a601,
              0xdea580d8, 0xda649d6f, 0xc423cd6a, 0xc0e2d0dd, 0xcda1f604,
              0xc960ebb3, 0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
              0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6,
              0x9ff77d71, 0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad,
              0x81b02d74, 0x857130c3, 0x5d8a9099, 0x594b8d2e, 0x5408abf7,
              0x50c9b640, 0x4e8ee645, 0x4a4ffbf2, 0x470cdd2b, 0x43cdc09c,
              0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8, 0x68860bfd,
              0x6c47164a, 0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e,
              0x18197087, 0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b,
              0x0fdc1bec, 0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088,
              0x2497d08d, 0x2056cd3a, 0x2d15ebe3, 0x29d4f654, 0xc5a92679,
              0xc1683bce, 0xcc2b1d17, 0xc8ea00a0, 0xd6ad50a5, 0xd26c4d12,
              0xdf2f6bcb, 0xdbee767c, 0xe3a1cbc1, 0xe760d676, 0xea23f0af,
              0xeee2ed18, 0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
              0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5,
              0x9e7d9662, 0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06,
              0xa6322bdf, 0xa2f33668, 0xbcb4666d, 0xb8757bda, 0xb5365d03,
              0xb1f740b4]

    UNSIGNED = lambda n: n & 0xffffffff

    def memcrc(b):
        n = len(b)
        i = c = s = 0
        for ch in b:
            c = ch
            tabidx = (s >> 24) ^ c
            s = UNSIGNED((s << 8)) ^ crctab[tabidx]

        while n:
            c = n & 0o377
            n = n >> 8
            s = UNSIGNED(s << 8) ^ crctab[(s >> 24) ^ c]
        return UNSIGNED(~s)

    buffer = open(file_to_check, 'rb').read()
    return memcrc(buffer)


###############################################################################

def dirs_and_files_in_url(url):
    """Return all listed files in specific URL
    """

    import urllib.request, urllib.error
    import re

    parse_re = re.compile(r'''(?<=href=["']).*?(?=["'])''', flags=re.UNICODE)
    try:
        html = urllib.request.urlopen(url).read().decode('utf-8')
    except IOError as e:
        status = 'error fetching {url}: {err}'.format(url=url, err=e)
        return None, status

    files = parse_re.findall(html)

    return files, 'ok'


# print dirs_and_files_in_url("http://aa.com123")

###############################################################################

def get_all_start_n_days_of_month(year, month, num_days=8):
    """Return all days for specific month and year with num_days interval
    """
    reference_date = date(year, 1, 1)

    # if reference_date.year > year:
    #    raise Exception("The date is bigger than reference date")

    tmp_date = reference_date
    while year != tmp_date.year:
        tmp_date += relativedelta(days=num_days)
    while month != tmp_date.month:
        tmp_date += relativedelta(days=num_days)

    list_days = []
    while month == tmp_date.month:
        list_days.append(tmp_date.day)
        tmp_date += relativedelta(days=num_days)

    return list_days


###############################################################################

class DateATD:
    """
    self.date_orig: original date
    self.date: date adjusted to interval N days
    self.is_start_month: True if nd_date start inside interval N days for this month
    self.is_end_month: True if nd_date end inside interval N days for this month
    """

    def __init__(self, date_str, date_type=None, freq_time=1):
        self.freq_time = freq_time
        self.set(date_str, date_type)

    def __str__(self):
        return str(self.date)

    def set(self, date_str, date_type=None):
        if len(date_str.split('-')) == 3:
            self.date_orig = parse(date_str).date()
        elif len(date_str.split('-')) == 2:
            days_list = get_all_start_n_days_of_month(int(date_str.split('-')[0]),
                                                      int(date_str.split('-')[1]),
                                                      num_days=self.freq_time)
            if date_type == "start":
                self.date_orig = date(int(date_str.split('-')[0]), int(date_str.split('-')[1]), int(days_list[0]))
            if date_type == "end":
                self.date_orig = date(int(date_str.split('-')[0]), int(date_str.split('-')[1]), int(days_list[-1]))
            if date_type is None:
                print("For date {0} with only year and month, you must set the type date ('start' or 'end').".format(
                    date_str))
                exit()
        else:
            print("Date {0} is not a valid date format, e.g. 2009-01-20 or 2009-01".format(date_str))
            exit()
        days_list = get_all_start_n_days_of_month(self.date_orig.year, self.date_orig.month, num_days=self.freq_time)
        for idx, day in enumerate(days_list):
            if day >= self.date_orig.day:
                self.date = date(self.date_orig.year, self.date_orig.month, day)
                break
        if self.date_orig.day > days_list[-1]:
            date_plus1 = self.date_orig + relativedelta(months=1)
            days_list_plus1 = get_all_start_n_days_of_month(date_plus1.year, date_plus1.month, num_days=self.freq_time)
            self.date = date(date_plus1.year, date_plus1.month, days_list_plus1[0])

        if date_type == "start":
            if self.date_orig < self.date:
                self.back()

        self.start_end_month()

    def start_end_month(self):
        days_list = get_all_start_n_days_of_month(self.date.year, self.date.month, num_days=self.freq_time)
        if self.date.day == days_list[0]:
            self.is_start_month = True
            self.is_end_month = False
        elif self.date.day == days_list[-1]:
            self.is_start_month = False
            self.is_end_month = True
        else:
            self.is_start_month = False
            self.is_end_month = False

    def next(self):
        days_list = get_all_start_n_days_of_month(self.date.year, self.date.month, num_days=self.freq_time)
        if self.date.day == days_list[-1]:
            date_plus1 = self.date + relativedelta(months=1)
            days_list_plus1 = get_all_start_n_days_of_month(date_plus1.year, date_plus1.month, num_days=self.freq_time)
            self.date = date(date_plus1.year, date_plus1.month, days_list_plus1[0])
        else:
            self.date = date(self.date.year, self.date.month, days_list[days_list.index(self.date.day) + 1])
        self.start_end_month()

    def back(self):
        days_list = get_all_start_n_days_of_month(self.date.year, self.date.month, num_days=self.freq_time)
        if self.date.day == days_list[0]:
            date_minus1 = self.date + relativedelta(months=-1)
            days_list_minus1 = get_all_start_n_days_of_month(date_minus1.year, date_minus1.month, num_days=self.freq_time)
            self.date = date(date_minus1.year, date_minus1.month, days_list_minus1[-1])
        else:
            self.date = date(self.date.year, self.date.month, days_list[days_list.index(self.date.day) - 1])
        self.start_end_month()


###############################################################################

def dir_date_name(start, end):
    """start and end must be instances of DateATD class
    """

    if start.date.year == end.date.year:
        year = start.date.year
    else:
        year = "{0}-{1}".format(start.date.year, str(end.date.year)[2::])
    #
    if start.is_start_month:
        month1 = "{0}".format(start.date.month)
    else:
        month1 = "{0}p".format(start.date.month)
    #
    if end.is_end_month:
        month2 = "{0}".format(end.date.month)
    else:
        month2 = "{0}p".format(end.date.month)
    return "{0}_({1}-{2})".format(year, month1, month2)


def update_working_directory(config_run):
    """Move/rename working directory with the last date of download made,
    only for download mode.
    """
    if config_run.working_directory == dir_date_name(config_run.start_date, config_run.target_date):
        return

    # close files
    if config_run.dnld_logfile is not None:
        config_run.dnld_logfile.close()

    # rename directory
    os.rename(config_run.working_directory,
              os.path.join(os.path.dirname(config_run.working_directory),
                           dir_date_name(config_run.start_date, config_run.target_date)))
    # update config variables
    config_run.working_directory = os.path.join(os.path.dirname(config_run.working_directory),
                                                dir_date_name(config_run.start_date, config_run.target_date))

    # re-set config path
    config_run.set_config_file()
    # re-set download paths
    config_run.download_path = os.path.join(config_run.working_directory, config_run.current_source, 'p0_download')
    # re-open log file
    config_run.dnld_logfile = open(os.path.join(config_run.download_path, 'download.log'), 'a')

    config_run.save()


###############################################################################

def get_pixel_size(raster_file):
    """Return the pixel size of raster, for square pixel else
    return absolute value of w-e pixel resolution.
    for n-s pixel resolution -> geo_transform[5]
    """
    gdal_file = gdal.Open(raster_file, gdal.GA_ReadOnly)
    geo_transform = gdal_file.GetGeoTransform()
    return abs(float(geo_transform[1]))


###############################################################################

def get_http_code(url):
    """Return the HTTP code from specific URL
    """
    # get/set user and password for FTP
    ftp_username = os.environ.get("ftp_username", "")
    ftp_password = os.environ.get("ftp_password", "")

    try:
        if not ftp_username == "" and not ftp_password == "":
            auth_handler = urllib.request.HTTPPasswordMgrWithDefaultRealm()
            auth_handler.add_password(None,
                                      uri='https://urs.earthdata.nasa.gov',
                                      user=ftp_username,
                                      passwd=ftp_password)
            opener = urllib.request.build_opener(urllib.request.HTTPBasicAuthHandler(auth_handler))
            urllib.request.install_opener(opener)

        conn = urllib.request.urlopen(url)

    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        return e.code
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        return -1
    else:
        # good url answer
        return 200
