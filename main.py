#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright © 2014 IDEAM and Patrimonio Natural
# Author: Xavier Corredor Llano <xcorredorl@ideam.gov.co>

import __init__
import os
from datetime import datetime, date
from math import floor

from ATD.lib import ConfigRun, send_mail
from ATD.download.files_download_scripts import modis


######################################## pre download ########################################

#global_path_to_run = '/home/xavier/Projects/SMDC/ATD/download/files_download_scripts/temp/'
global_path_to_run = '/Modelo_Raster/Modelos/Modelos1/Alertas_Temp_Deforest/'

config_run = ConfigRun(global_path_to_run)
config_run.load()

if config_run.months_made == config_run.months_to_run:
    print 'Months made is equal to months to process, maybe the download is finished.'
    print 'Starting new period to process...'

    config_run.months_made = 0
    if config_run.month_to_process == 12:
        config_run.year_to_run += 1
        config_run.month_to_process = 1
## check if the period to process not is bigger than now
if date(config_run.year_to_run, config_run.month_to_process,1) > date.today():
    print "Error the date to download is bigger than the current date"
    exit()

## added the directory period
init_period = int((floor((config_run.month_to_process-1)/config_run.months_to_run))*config_run.months_to_run+1)
end_period = init_period + config_run.months_to_run - 1
dir_process = "{0}_({1}-{2})".format(config_run.year_to_run, init_period, end_period)
print dir_process

path_to_run = os.path.join(global_path_to_run, dir_process)

########################################## download ##########################################

dnld_errors_A1, status_file_A1 = modis.download('MOD09A1', path_to_run, config_run.year_to_run, config_run.month_to_process)
dnld_errors_Q1, status_file_Q1 = modis.download('MOD09Q1', path_to_run, config_run.year_to_run, config_run.month_to_process)

######################################## post download #######################################
config_run.months_made += 1

mail_subject = "Reporte de la descarga de Aler.Temp.Defor. para {0}-{1} ({2}/{3})".format(config_run.year_to_run,
                                                                                      config_run.month_to_process,
                                                                                      config_run.months_made,
                                                                                      config_run.months_to_run)
mail_body = \
    '\n{0}\n\nEste es el reporte automático de la descarga de las\n' \
    'Alertas Tempranas de Deforestación\n\n' \
    'Archivos modis MOD09A1 y MOD09Q1 para el {1}-{2}\n\n' \
    'Realizados {3} mes(es) de {4}\n\n' \
    'La ruta de almacenamiento de los resultados:\n' \
    '   (SAN): {5}\n'.format(datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                         config_run.year_to_run,
                         config_run.month_to_process,
                         config_run.months_made,
                         config_run.months_to_run,
                         path_to_run)

if config_run.months_made == config_run.months_to_run:
    mail_body += '\nLa descarga se ha completado!.\n'

if dnld_errors_A1 > 0 or dnld_errors_Q1 > 0:
    mail_body += '\nEl log de descarga reporto algun(os) problema(s)\n'

mail_body += '\nAdjunto se envían los logs del reporte de descarga.\n'

send_mail('xcorredorl@ideam.gov.co',
          'xcorredorl@ideam.gov.co, juanramirez85@gmail.com, liseth.rodriguez@gmail.com',
          mail_subject,
          mail_body,
          [status_file_A1,status_file_Q1 ])

config_run.month_to_process += 1
config_run.save()

######################################## TiSeg process ########################################
#


######################################### MRT process #########################################
#





exit()