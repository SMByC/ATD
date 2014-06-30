#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from datetime import datetime
import os

import shutil
from subprocess import call
from lib import datetime_format


def run(config_run):

    if config_run.p4_stats not in [None, 'None']:
        msg = '\nWarning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg+'\n')
        print msg

    dir_process = os.path.join(config_run.abs_path_dir, config_run.process_name)

    source_path = os.path.join(config_run.abs_path_dir, 'p3_nodata')

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.'.format(source_path)
        config_run.process_logfile.write(msg+'\n')
        print msg
        # save in setting
        config_run.p4_stats = 'with errors! - '+datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    list_files = []
    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.tif']
            for file in files:
                file_process = os.path.join(root, file)
                list_files.append(file_process)

    msg = 'Creating script file in R'
    config_run.process_logfile.write(msg)
    config_run.process_logfile.flush()
    print msg

    file_R = script_R(list_files, dir_process)

    return_code = call(["Rscript", file_R])

    if return_code == 0:  # successfully
        msg = 'was converted successfully'
    else:
        msg = '\nError: The R script return a error, please check\n' \
              'error message above, likely the files not were\n' \
              'processed successfully.'

    config_run.process_logfile.write(msg+'\n')
    print msg

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                           'with errors! ' if return_code != 0 else '',
                                                           datetime_format(datetime.today()))

    config_run.process_logfile.write(msg+'\n')
    print msg
    # save in setting
    config_run.p4_stats = 'done - '+datetime_format(datetime.today())
    config_run.save()


def script_R(list_files, dest):

    if not os.path.isdir(dest):
        os.makedirs(dest)

    r_file = open(os.path.join(dest, 'script_R.r'), 'w')
    ## start script

    str_r = '''
    ##### MEDIANA, MEDIA y SD DE IMAGENES DE SALIDA REPROYECTADAS Y SIN VALORES NEGATIVOS #####

    #Load libraries
    rm(list = ls(all = TRUE))

    require(sp)
    require(raster)
    require(rgdal)
    require(spatial)
    require(plyr)
    require(doSNOW)

    registerDoSNOW(makeCluster(2, type = "SOCK"))
    getDoParWorkers()
    getDoParName()

    '''

    ## mediana
    mediana_dir = os.path.join(dest, 'mediana')
    if not os.path.isdir(mediana_dir):
        os.makedirs(mediana_dir)

    str_r += '##### MEDIANA DE IMAGENES DE SALIDA REPROYECTADAS Y SIN VALORES NEGATIVOS #####'

    for file_process in list_files:
        name_file = os.path.basename(file_process)
        str_r += """
        rm(list = ls(all = TRUE))
        # Nombre y ruta del stack raster con la serie (1)
        inImage<-'{in_file}'
        # nombre y ruta de la imagen salida (2)
        outImage <-'{out_file}'

        satImage <- stack(inImage)
            for (b in 1:nlayers(satImage)) {{ NAvalue(satImage@layers[[b]]) <- 0 }}
        med_prep <- function(x, ...) {{ x[x==0] <- NA; median(x, na.rm=TRUE) }}
        mediana <- calc(satImage, med_prep)
        str(mediana)
        mediana
        writeRaster(mediana, outImage,  bandorder='BIL', datatype='INT2S', format="GTiff")

        """.format(in_file=file_process,
                    out_file=os.path.join(mediana_dir, os.path.splitext(name_file)[0]+'_mediana.tif'))

    ## media
    media_dir = os.path.join(dest, 'media')
    if not os.path.isdir(media_dir):
        os.makedirs(media_dir)

    str_r += '##### MEDIA DE IMAGENES DE SALIDA REPROYECTADAS Y SIN VALORES NEGATIVOS #####'

    for file_process in list_files:
        name_file = os.path.basename(file_process)
        str_r += """
        rm(list = ls(all = TRUE))
        # Nombre y ruta del stack raster con la serie (1)
        inImage<-'{in_file}'
        # nombre y ruta de la imagen salida (2)
        outImage <-'{out_file}'

        satImage <- stack(inImage)
            for (b in 1:nlayers(satImage)) {{ NAvalue(satImage@layers[[b]]) <- 0 }}
        mean_prep <- function(x, ...) {{ x[x==0] <- NA; mean(x, na.rm=TRUE) }}
        mean <- calc(satImage, mean_prep)
        str(mean)
        mean
        writeRaster(mean, outImage,  bandorder='BIL', datatype='INT2S', format="GTiff")

        """.format(in_file=file_process,
                    out_file=os.path.join(media_dir, os.path.splitext(name_file)[0]+'_media.tif'))

    ## standard deviation
    sd_dir = os.path.join(dest, 'sd')
    if not os.path.isdir(sd_dir):
        os.makedirs(sd_dir)

    str_r += '##### DESVIACIÓN ESTANDAR DE IMAGENES DE SALIDA REPROYECTADAS Y SIN VALORES NEGATIVOS #####'

    for file_process in list_files:
        name_file = os.path.basename(file_process)
        str_r += """
        rm(list = ls(all = TRUE))
        # Nombre y ruta del stack raster con la serie (1)
        inImage<-'{in_file}'
        # nombre y ruta de la imagen salida (2)
        outImage <-'{out_file}'

        satImage <- stack(inImage)
            for (b in 1:nlayers(satImage)) {{ NAvalue(satImage@layers[[b]]) <- 0 }}
        sd_prep <- function(x, ...) {{ x[x==0] <- NA; sd(x, na.rm=TRUE) }}
        sd <- calc(satImage, sd_prep)
        str(sd)
        sd
        writeRaster(sd, outImage,  bandorder='BIL', datatype='INT2S', format="GTiff")

        """.format(in_file=file_process,
                    out_file=os.path.join(sd_dir, os.path.splitext(name_file)[0]+'_sd.tif'))

    r_file.write(str_r)
    r_file.close()

    return os.path.join(dest, 'script_R.r')