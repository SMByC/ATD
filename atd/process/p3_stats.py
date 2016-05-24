#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import numpy as np
import shutil
from datetime import datetime
from scipy.stats import variation

try:  # old
    from osgeo import gdal
    from osgeo import osr
except ImportError:  # new
    import gdal
    import osr

from atd.lib import datetime_format


def run(config_run):
    if config_run.p3_stats not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p2_reproj')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p3_stats = 'with errors! - ' + datetime_format(datetime.today())
        config_run.save()
        return

    if os.path.isdir(dir_process):
        shutil.rmtree(dir_process)

    # process file by file
    def process():
        for root, dirs, files in os.walk(source_path):
            if len(files) != 0:
                files = [x for x in files if x.endswith(('.tif', '.TIF'))]
                for file in files:
                    ##############
                    # Calculate the median statistical
                    msg = 'Calculating the median statistical for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # median directory
                    median_dir = os.path.join(dir_process, 'median')
                    if not os.path.isdir(median_dir):
                        os.makedirs(median_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(median_dir, os.path.splitext(file)[0] + '_median.tif')

                    try:
                        statistics('median', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating median statistic\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

                    ##############
                    # Calculate the mean statistical
                    msg = 'Calculating the mean statistical for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # mean directory
                    mean_dir = os.path.join(dir_process, 'mean')
                    if not os.path.isdir(mean_dir):
                        os.makedirs(mean_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(mean_dir, os.path.splitext(file)[0] + '_mean.tif')

                    try:
                        statistics('mean', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating mean statistic\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

                    ##############
                    # Calculate the standard deviation
                    msg = 'Calculating the standard deviation for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # standard deviation directory
                    std_dir = os.path.join(dir_process, 'std')
                    if not os.path.isdir(std_dir):
                        os.makedirs(std_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(std_dir, os.path.splitext(file)[0] + '_std.tif')

                    try:
                        statistics('std', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating standard deviation\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

                    ##############
                    # Calculate the valid data
                    # this count the valid data (no nans) across the layers (time axis) in percentage (0-100%)
                    msg = 'Calculating the valid data for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # valid data directory
                    vd_dir = os.path.join(dir_process, 'valid_data')
                    if not os.path.isdir(vd_dir):
                        os.makedirs(vd_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(vd_dir, os.path.splitext(file)[0] + '_valid_data.tif')

                    try:
                        statistics('valid_data', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating valid data\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

                    ##############
                    # Calculate the signal-to-noise ratio
                    # this signal-to-noise ratio defined as the mean divided by the standard deviation.
                    msg = 'Calculating the signal-to-noise ratio for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # valid data directory
                    snr_dir = os.path.join(dir_process, 'snr')
                    if not os.path.isdir(snr_dir):
                        os.makedirs(snr_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(snr_dir, os.path.splitext(file)[0] + '_snr.tif')

                    try:
                        statistics('snr', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating signal-to-noise ratio\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

                    ##############
                    # Computes the coefficient of variation, the ratio of the biased standard
                    # deviation to the mean.
                    msg = 'Calculating the coefficient of variation for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='')

                    # coefficient of variation directory
                    coeff_var_dir = os.path.join(dir_process, 'coeff_var')
                    if not os.path.isdir(coeff_var_dir):
                        os.makedirs(coeff_var_dir)

                    in_file = os.path.join(root, file)
                    out_file = os.path.join(coeff_var_dir, os.path.splitext(file)[0] + '_coeff_var.tif')

                    try:
                        statistics('coeff_var', in_file, out_file)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating coefficient of variation\n' + error
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        return msg

        return 0

    return_code = process()

    # finishing the process
    msg = '\nThe process {0} completed {1}- ({2})'.format(config_run.process_name,
                                                          'with errors! ' if return_code != 0 else '',
                                                          datetime_format(datetime.today()))

    config_run.process_logfile.write(msg + '\n')
    print(msg)
    # save in setting
    config_run.p3_stats = ('with errors! - ' if return_code != 0 else 'done - ') + datetime_format(datetime.today())
    config_run.save()


def get_geo_info(file_name):
    """Function to read the original file's projection
    """
    source_dataset = gdal.Open(file_name, gdal.GA_ReadOnly)
    no_data_value = source_dataset.GetRasterBand(1).GetNoDataValue()
    xsize = source_dataset.RasterXSize
    ysize = source_dataset.RasterYSize
    geo_trans = source_dataset.GetGeoTransform()
    projection = osr.SpatialReference()
    projection.ImportFromWkt(source_dataset.GetProjectionRef())
    data_type = source_dataset.GetRasterBand(1).DataType
    data_type = gdal.GetDataTypeName(data_type)
    return no_data_value, xsize, ysize, geo_trans, projection, data_type


def statistics(stat, infile, outfile):
    """Calculate the statistics
    """

    # Open the original file
    dataset = gdal.Open(infile, gdal.GA_ReadOnly)
    # get the projection information
    no_data_value, xsize, ysize, geo_trans, projection, data_type = get_geo_info(infile)

    # loop thru bands of raster and append each band of data to 'layers'
    layers = []
    num_layers = dataset.RasterCount
    for i in range(1, num_layers + 1):
        raster_band = dataset.GetRasterBand(i).ReadAsArray()
        # raster_band[raster_band == 0] = np.nan
        raster_band = raster_band.astype(float)
        # convert the no data value to NaN
        raster_band[raster_band == no_data_value] = np.nan
        layers.append(raster_band)

    # dstack will take a number of n by m in tuple or list and stack them
    # in the 3rd dimension so you end up with raster_stack being n by m by i,
    # where i is the number of bands
    raster_stack = np.dstack(layers)

    # call built in numpy statistical functions, with a specified axis. if
    # axis=2 means it will calculate along the 'depth' axis, per pixel.
    # with the return being n by m, the shape of each band.
    #
    # Calculate the median statistical
    if stat == 'median':
        new_array = np.nanmedian(raster_stack, axis=2)
    # Calculate the mean statistical
    if stat == 'mean':
        new_array = np.nanmean(raster_stack, axis=2)
    # Calculate the standard deviation
    if stat == 'std':
        new_array = np.nanstd(raster_stack, axis=2)
    # Calculate the valid data
    if stat == 'valid_data':
        # calculate the number of valid data used in statistics products in percentage (0-100%),
        # this count the valid data (no nans) across the layers (time axis)
        new_array = (num_layers - np.isnan(raster_stack).sum(axis=2))*100/num_layers
    # Calculate the signal-to-noise ratio
    if stat == 'snr':
        # this signal-to-noise ratio defined as the mean divided by the standard deviation.
        m = np.nanmean(raster_stack, axis=2)
        sd = np.nanstd(raster_stack, axis=2, ddof=0)
        new_array = np.where(sd == 0, 0, m / sd)
    # Calculate the coefficient of variation
    if stat == 'coeff_var':
        # the ratio of the biased standard deviation to the mean
        new_array = variation(raster_stack, axis=2, nan_policy='omit')

    #### create the output geo tif
    # Set up the GTiff driver
    driver = gdal.GetDriverByName('GTiff')

    new_dataset = driver.Create(outfile, xsize, ysize, 1, gdal.GDT_Float32,
                                ["COMPRESS=LZW", "PREDICTOR=2", "TILED=YES"])
    # the '1' is for band 1
    new_dataset.SetGeoTransform(geo_trans)
    new_dataset.SetProjection(projection.ExportToWkt())
    # Write the array
    new_dataset.GetRasterBand(1).WriteArray(new_array)
    new_dataset.GetRasterBand(1).SetNoDataValue(np.nan)