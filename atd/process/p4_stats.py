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
from scipy.stats import variation, ss

try:  # old
    from osgeo import gdal
    from osgeo import osr
except ImportError:  # new
    import gdal
    import osr

from atd.lib import datetime_format


def run(config_run):
    if config_run.p4_stats not in [None, 'None']:
        msg = 'Warning: The process {0} was executed before\n'.format(config_run.process_name)
        config_run.process_logfile.write(msg)
        print(msg)

    # optional argument prev_rundir only for p4_stats for computes all statistics
    if config_run.prev_rundir:
        msg = "p4_stats will make some statistics with previous run, located in:\n\t" + \
              os.path.join(config_run.prev_rundir, os.path.basename(config_run.working_directory)) + '\n'
        config_run.process_logfile.write(msg)
        print(msg)
        # define and check files in previous run directory
        previous_p3_mosaic_dir = os.path.join(config_run.prev_rundir, os.path.basename(config_run.working_directory), "p3_mosaic")
        if not os.path.isdir(previous_p3_mosaic_dir):
            msg = '\nWARNING: The directory of previous p3_mosaic run: {0}\n' \
                  'not exist, please run the previous process before it.\n'\
                  'continue but some statistics don\'t will processed.\n'.format(previous_p3_mosaic_dir)
            config_run.process_logfile.write(msg)
            print(msg)
            config_run.prev_rundir = None
    else:
        msg = '\nWARNING: Not defined \'--prev_rundir\' directory of previous run,\n' \
              'continue but some statistics don\'t will processed.\n'
        config_run.process_logfile.write(msg)
        print(msg)

    source_path = os.path.join(config_run.working_directory, 'p3_mosaic')
    dir_process = os.path.join(config_run.working_directory, config_run.process_name)

    if not os.path.isdir(source_path):
        msg = '\nError: The directory of previous process: {0}\n' \
              'not exist, please run the previous process before it.\n'.format(source_path)
        config_run.process_logfile.write(msg)
        print(msg)
        # save in setting
        config_run.p4_stats = 'with errors! - ' + datetime_format(datetime.today())
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

                    ##############
                    # Calculate the Pearson's correlation coefficient between this time series (x)
                    # with the previous time series (y): pearson_corr = covar(x,y)/(std(x)*std(y))
                    if config_run.prev_rundir:
                        msg = 'Calculating the Pearson\'s correlation coefficient for {0}: '.format(file)
                        config_run.process_logfile.write(msg)
                        config_run.process_logfile.flush()
                        print(msg, end='')

                        # Pearson's correlation coefficient directory
                        pearson_corr_dir = os.path.join(dir_process, 'pearson_corr')
                        if not os.path.isdir(pearson_corr_dir):
                            os.makedirs(pearson_corr_dir)

                        in_file = os.path.join(root, file)
                        out_file = os.path.join(pearson_corr_dir, os.path.splitext(file)[0] + '_pearson_corr.tif')

                        try:
                            statistics('pearson_corr', in_file, out_file, previous_p3_mosaic_dir)
                            msg = 'OK'
                            config_run.process_logfile.write(msg + '\n')
                            print(msg)
                        except Exception as error:
                            msg = 'FAIL\nError: While calculating Pearson\'s correlation coefficient\n' + error
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
    config_run.p4_stats = ('with errors! - ' if return_code != 0 else 'done - ') + datetime_format(datetime.today())
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


def bands2layerstack(img_file, convert_nd2nan=True):
    """Convert the bands of image in a numpy 3rd dimension array stack
    where the z axis are locate the bands
    """
    # Open the original file
    dataset = gdal.Open(img_file, gdal.GA_ReadOnly)
    # loop thru bands of raster and append each band of data to 'layers'
    layers = []
    num_layers = dataset.RasterCount
    for i in range(1, num_layers + 1):
        raster_band = dataset.GetRasterBand(i).ReadAsArray()
        # raster_band[raster_band == 0] = np.nan
        raster_band = raster_band.astype(float)
        if convert_nd2nan:
            # convert the no data value to NaN
            no_data_value = dataset.GetRasterBand(i).GetNoDataValue()
            raster_band[raster_band == no_data_value] = np.nan
            layers.append(raster_band)

    # dstack will take a number of n by m in tuple or list and stack them
    # in the 3rd dimension so you end up with raster_stack being n by m by i,
    # where i is the number of bands
    raster_stack = np.dstack(layers)
    return raster_stack


def statistics(stat, infile, outfile, previous_p3_mosaic_dir=None):
    """Calculate the statistics
    """

    # Open the original file
    dataset = gdal.Open(infile, gdal.GA_ReadOnly)
    num_layers = dataset.RasterCount
    # get the projection information
    no_data_value, xsize, ysize, geo_trans, projection, data_type = get_geo_info(infile)

    # get the numpy 3rd dimension array stack of the bands of image
    raster_stack = bands2layerstack(infile)

    # define the default output type format
    output_type = gdal.GDT_Float32

    # call built in numpy statistical functions, with a specified axis. if
    # axis=2 means it will calculate along the 'depth' axis, per pixel.
    # with the return being n by m, the shape of each band.
    #
    # Calculate the median statistical
    if stat == 'median':
        new_array = np.nanmedian(raster_stack, axis=2)
        output_type = gdal.GDT_UInt16
    # Calculate the mean statistical
    if stat == 'mean':
        new_array = np.nanmean(raster_stack, axis=2)
        output_type = gdal.GDT_UInt16
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
    # Calculate the Pearson's correlation coefficient
    if stat == 'pearson_corr':
        # https://github.com/scipy/scipy/blob/v0.14.0/scipy/stats/stats.py#L2392
        # get array of the previous mean file
        previous_dataset_file = os.path.join(previous_p3_mosaic_dir,
                                             os.path.basename(outfile).split('_pearson_corr.tif')[0] + '.tif')

        # get the numpy 3rd dimension array stack of the bands of image
        previous_raster_stack = bands2layerstack(previous_dataset_file)

        # raster_stack and previous_raster_stack should have same length in all axis
        if raster_stack.shape != previous_raster_stack.shape:
            z_rs = raster_stack.shape[2]
            z_prs = previous_raster_stack.shape[2]

            if z_rs > z_prs:
                raster_stack = np.delete(raster_stack, np.s_[z_prs-z_rs:], 2)
            if z_prs > z_rs:
                previous_raster_stack = np.delete(previous_raster_stack, np.s_[z_rs-z_prs:], 2)

        # propagate the nan values across the pair values in the same position for the
        # two raster in both directions
        mask1 = np.isnan(raster_stack)
        mask2 = np.isnan(previous_raster_stack)
        combined_mask = mask1 | mask2
        raster_stack = np.where(combined_mask, np.nan, raster_stack)
        previous_raster_stack = np.where(combined_mask, np.nan, previous_raster_stack)
        del mask1, mask2, combined_mask

        mean_rs = np.nanmean(raster_stack, axis=2, keepdims=True)
        mean_prs = np.nanmean(previous_raster_stack, axis=2, keepdims=True)
        m_rs = np.nan_to_num(raster_stack - mean_rs)
        m_prs = np.nan_to_num(previous_raster_stack - mean_prs)
        r_num = np.add.reduce(m_rs * m_prs, axis=2)
        r_den = np.sqrt(ss(m_rs, axis=2) * ss(m_prs, axis=2))
        r = r_num / r_den

        # return the r coefficient -1 to 1
        new_array = r

    #### create the output geo tif
    # Set up the GTiff driver
    driver = gdal.GetDriverByName('GTiff')

    new_dataset = driver.Create(outfile, xsize, ysize, 1, output_type,
                                ["COMPRESS=LZW", "PREDICTOR=2", "TILED=YES"])
    # the '1' is for band 1
    new_dataset.SetGeoTransform(geo_trans)
    new_dataset.SetProjection(projection.ExportToWkt())
    # Write the array
    new_dataset.GetRasterBand(1).WriteArray(new_array)
    new_dataset.GetRasterBand(1).SetNoDataValue(np.nan)