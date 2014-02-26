#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014 IDEAM
# Author: Xavier Corredor <xcorredorl@ideam.gov.co>

import __init__
from datetime import datetime
from ATD.lib import ConfigRun, send_mail
from ATD.download.files_download_scripts import modis


path_to_run = '/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/'

config_run = ConfigRun(path_to_run)
config_run.load()

if config_run.months_made == config_run.months_to_run:
    print 'Months made is equal to months to process, maybe the download is finished.'
    exit()

dnld_errors_A1, status_file_A1 = modis.download('MOD09A1', path_to_run, config_run.year_to_run, config_run.month_to_process)
dnld_errors_Q1, status_file_Q1 = modis.download('MOD09Q1', path_to_run, config_run.year_to_run, config_run.month_to_process)

config_run.months_made += 1

mail_subject = "Reporte de la descarga de Aler.Temp.Defor. para {0}-{1} ({2}/{3})".format(config_run.year_to_run,
                                                                                      config_run.month_to_process,
                                                                                      config_run.months_made,
                                                                                      config_run.months_to_run)
mail_body = \
    '\n{0}\n\nEste es el reporte automático de la descarga de las\n' \
    'Alertas Tempranas de Deforestación\n\n' \
    'Archivos modis MOD09A1 y MOD09Q1 para el {1}-{2}\n\n' \
    'Realizados {3} meses de {4}\n'.format(datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                                         config_run.year_to_run,
                                         config_run.month_to_process,
                                         config_run.months_made,
                                         config_run.months_to_run)

if config_run.months_made == config_run.months_to_run:
    mail_body += '\nLa descarga se ha completado!.\n'

if dnld_errors_A1 > 0 or dnld_errors_Q1 > 0:
    mail_body += '\nEl log de descarga reporto algun(os) problema(s)\n'

mail_body += '\nAdjunto se envían los logs del reporte de descarga\n' \
             'para éste mes, por favor reviselos.'

send_mail('xcorredorl@ideam.gov.co',
          'xcorredorl@ideam.gov.co',
          mail_subject,
          mail_body,
          [status_file_A1,status_file_Q1 ])

config_run.month_to_process += 1
config_run.save()
exit()