#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import numpy
import shutil
from datetime import datetime

try:
    from osgeo import gdal

    gdal.TermProgress = gdal.TermProgress_nocb
except ImportError:
    import gdal

from atd.lib import datetime_format


def run(config_run, name_process):
    if config_run.process_[name_process] not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    before_name_process = config_run.list_of_process[config_run.list_of_process.index(name_process) - 1]
    source_path = os.path.join(config_run.working_directory, before_name_process)
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.process_[name_process] = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    any_error = False
    # process file by file
    for root, dirs, files in os.walk(source_path):
        if len(files) != 0:
            files = [x for x in files if x[-4::] == '.tif']
            for file in files:
                infile = os.path.join(root, file)
                # outfile = os.path.dirname(os.path.join(dir_process, os.path.join(root, file).split('/p2_mrt/')[-1]))
                outfile = infile.replace(before_name_process, name_process)

                msg = 'Processing file {0} converting negative values to zero: '.format(file)
                config_run.process_logfile.write(msg)
                config_run.process_logfile.flush()
                print(msg)

                msg = negative_to_zero(infile, outfile)
                if msg is True:
                    msg = 'was converted successfully'
                else:
                    any_error = True
                config_run.process_logfile.write(msg + '\n')
                print(msg)

    # finishing the process
    msg = '\nThe process {0} completed {1} - ({2})'.format(config_run.process_name,
                                                           'with errors! ' if any_error else '',
                                                           datetime_format(datetime.today()))
    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.process_[name_process] = 'with errors! - ' if any_error else 'done - ' + datetime_format(
        datetime.today())
    if name_process == 'p3_nodata':
        config_run.p3_nodata = config_run.process_[name_process]
    if name_process == 'p5_nodata':
        config_run.p5_nodata = config_run.process_[name_process]
    config_run.save()


def negative_to_zero(infile, outfile):
    """Convert all negative pixel values to zero
    """

    if not os.path.isdir(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))

    outNoData = 0
    format = 'GTiff'
    type = gdal.GDT_Int16

    try:
        indataset = gdal.Open(infile, gdal.GA_ReadOnly)

        out_driver = gdal.GetDriverByName(format)
        outdataset = out_driver.Create(outfile, indataset.RasterXSize, indataset.RasterYSize, indataset.RasterCount,
                                       type)

        gt = indataset.GetGeoTransform()
        if gt is not None and gt != (0.0, 1.0, 0.0, 0.0, 0.0, 1.0):
            outdataset.SetGeoTransform(gt)

        prj = indataset.GetProjectionRef()
        if prj is not None and len(prj) > 0:
            outdataset.SetProjection(prj)

        for iBand in range(1, indataset.RasterCount + 1):
            inband = indataset.GetRasterBand(iBand)
            outband = outdataset.GetRasterBand(iBand)

            for i in range(inband.YSize - 1, -1, -1):
                scanline = inband.ReadAsArray(0, i, inband.XSize, 1, inband.XSize, 1)
                scanline = numpy.choose(scanline < 0, (scanline, outNoData))
                outband.WriteArray(scanline, 0, i)

        return True
    except OSError as error:
        return str(error)
