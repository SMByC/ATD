#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

import os
import tempfile
import numpy as np
import shutil
from joblib import load, dump, Parallel, delayed
from datetime import datetime
from scipy.stats import variation, ss
from math import ceil, floor
from itertools import product

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
                    # load file as a list of bands and saved it in memmap files
                    in_file = os.path.join(root, file)
                    # define temp dir and memmap raster to save
                    tmp_folder = tempfile.mkdtemp(dir=config_run.tmp_dir)
                    # Open the original file
                    dataset = gdal.Open(in_file, gdal.GA_ReadOnly)
                    # loop thru bands of raster and append each band of data to 'layers'
                    raster_stack = []
                    num_layers = dataset.RasterCount
                    for i in range(1, num_layers + 1):
                        raster_band = dataset.GetRasterBand(i).ReadAsArray()
                        # raster_band[raster_band == 0] = np.nan
                        raster_band = raster_band.astype(float)
                        # convert the no data value to NaN
                        no_data_value = dataset.GetRasterBand(i).GetNoDataValue()
                        raster_band[raster_band == no_data_value] = np.nan
                        # dumb
                        raster_band_file = os.path.join(tmp_folder, str(i))
                        dump(raster_band, raster_band_file, compress=0)  # compress=('lzma', 3)
                        # load and save the raster from memmap disk cache
                        raster_stack.append(load(raster_band_file, mmap_mode='r'))
                        del raster_band_file, raster_band

                    ##############
                    # Calculate the median statistical
                    msg = 'Calculating the median statistical for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # median directory
                    median_dir = os.path.join(dir_process, 'median')
                    if not os.path.isdir(median_dir):
                        os.makedirs(median_dir)

                    out_file = os.path.join(median_dir, os.path.splitext(file)[0] + '_median.tif')

                    try:
                        multiprocess_statistic('median', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating median statistic\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Calculate the mean statistical
                    msg = 'Calculating the mean statistical for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # mean directory
                    mean_dir = os.path.join(dir_process, 'mean')
                    if not os.path.isdir(mean_dir):
                        os.makedirs(mean_dir)

                    out_file = os.path.join(mean_dir, os.path.splitext(file)[0] + '_mean.tif')

                    try:
                        multiprocess_statistic('mean', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating mean statistic\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Calculate the standard deviation
                    msg = 'Calculating the standard deviation for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # standard deviation directory
                    std_dir = os.path.join(dir_process, 'std')
                    if not os.path.isdir(std_dir):
                        os.makedirs(std_dir)

                    out_file = os.path.join(std_dir, os.path.splitext(file)[0] + '_std.tif')

                    try:
                        multiprocess_statistic('std', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating standard deviation\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Calculate the valid data
                    # this count the valid data (no nans) across the layers (time axis) in percentage (0-100%)
                    msg = 'Calculating the valid data for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # valid data directory
                    vd_dir = os.path.join(dir_process, 'valid_data')
                    if not os.path.isdir(vd_dir):
                        os.makedirs(vd_dir)

                    out_file = os.path.join(vd_dir, os.path.splitext(file)[0] + '_valid_data.tif')

                    try:
                        multiprocess_statistic('valid_data', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating valid data\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Calculate the signal-to-noise ratio
                    # this signal-to-noise ratio defined as the mean divided by the standard deviation.
                    msg = 'Calculating the signal-to-noise ratio for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # valid data directory
                    snr_dir = os.path.join(dir_process, 'snr')
                    if not os.path.isdir(snr_dir):
                        os.makedirs(snr_dir)

                    out_file = os.path.join(snr_dir, os.path.splitext(file)[0] + '_snr.tif')

                    try:
                        multiprocess_statistic('snr', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating signal-to-noise ratio\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Computes the coefficient of variation, the ratio of the biased standard
                    # deviation to the mean.
                    msg = 'Calculating the coefficient of variation for {0}: '.format(file)
                    config_run.process_logfile.write(msg)
                    config_run.process_logfile.flush()
                    print(msg, end='', flush=True)

                    # coefficient of variation directory
                    coeff_var_dir = os.path.join(dir_process, 'coeff_var')
                    if not os.path.isdir(coeff_var_dir):
                        os.makedirs(coeff_var_dir)

                    out_file = os.path.join(coeff_var_dir, os.path.splitext(file)[0] + '_coeff_var.tif')

                    try:
                        multiprocess_statistic('coeff_var', in_file, raster_stack, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                    except Exception as error:
                        msg = 'FAIL\nError: While calculating coefficient of variation\n' + str(error)
                        config_run.process_logfile.write(msg + '\n')
                        print(msg)
                        shutil.rmtree(tmp_folder)
                        return msg

                    ##############
                    # Calculate the Pearson's correlation coefficient between this time series (x)
                    # with the previous time series (y): pearson_corr = covar(x,y)/(std(x)*std(y))
                    if config_run.prev_rundir:
                        msg = 'Calculating the Pearson\'s correlation coefficient for {0}: '.format(file)
                        config_run.process_logfile.write(msg)
                        config_run.process_logfile.flush()
                        print(msg, end='', flush=True)

                        # Pearson's correlation coefficient directory
                        pearson_corr_dir = os.path.join(dir_process, 'pearson_corr')
                        if not os.path.isdir(pearson_corr_dir):
                            os.makedirs(pearson_corr_dir)

                        out_file = os.path.join(pearson_corr_dir, os.path.splitext(file)[0] + '_pearson_corr.tif')

                        #######
                        # Open and load the previous rundir of mosaic file
                        # get array of the previous mean file
                        previous_dataset_file = \
                            os.path.join(previous_p3_mosaic_dir, os.path.basename(out_file).split('_pearson_corr.tif')[0] + '.tif')
                        # define temp dir and memmap raster to save
                        tmp_folder_prev_rundir = tempfile.mkdtemp(dir=config_run.tmp_dir)
                        # Open the original file
                        dataset = gdal.Open(previous_dataset_file, gdal.GA_ReadOnly)
                        # loop thru bands of raster and append each band of data to 'layers'
                        prev_raster_stack = []
                        num_layers = dataset.RasterCount
                        for i in range(1, num_layers + 1):
                            raster_band = dataset.GetRasterBand(i).ReadAsArray()
                            # raster_band[raster_band == 0] = np.nan
                            raster_band = raster_band.astype(float)
                            # convert the no data value to NaN
                            no_data_value = dataset.GetRasterBand(i).GetNoDataValue()
                            raster_band[raster_band == no_data_value] = np.nan
                            # dumb
                            raster_band_file = os.path.join(tmp_folder_prev_rundir, str(i))
                            dump(raster_band, raster_band_file, compress=0)  # compress=('lzma', 3)
                            # load and save the raster from memmap disk cache
                            prev_raster_stack.append(load(raster_band_file, mmap_mode='r'))
                            del raster_band_file, raster_band

                        try:
                            multiprocess_statistic('pearson_corr', in_file, raster_stack, out_file, prev_raster_stack,
                                                   config_run.number_of_processes, config_run.tmp_dir)
                            msg = 'OK'
                            config_run.process_logfile.write(msg + '\n')
                            print(msg)
                        except Exception as error:
                            msg = 'FAIL\nError: While calculating Pearson\'s correlation coefficient\n' + str(error)
                            config_run.process_logfile.write(msg + '\n')
                            print(msg)
                            shutil.rmtree(tmp_folder)
                            return msg

                        # clean
                        del prev_raster_stack
                        shutil.rmtree(tmp_folder_prev_rundir)

                    # clean
                    del raster_stack
                    shutil.rmtree(tmp_folder)

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
    x_size = source_dataset.RasterXSize
    y_size = source_dataset.RasterYSize
    geo_trans = source_dataset.GetGeoTransform()
    projection = osr.SpatialReference()
    projection.ImportFromWkt(source_dataset.GetProjectionRef())
    data_type = source_dataset.GetRasterBand(1).DataType
    data_type = gdal.GetDataTypeName(data_type)
    return no_data_value, x_size, y_size, geo_trans, projection, data_type


def statistic(stat, raster_stack, output_array, x_chunk, y_chunk, prev_raster_stack=None):

    # get the numpy 3rd dimension array stack of the bands in chunks (x_chunk and y_chunk)
    raster_layerstack = np.dstack([band[np.ix_(y_chunk, x_chunk)] for band in raster_stack])

    # call built in numpy statistical functions, with a specified axis. if
    # axis=2 means it will calculate along the 'depth' axis, per pixel.
    # with the return being n by m, the shape of each band.
    #
    # Calculate the median statistical
    if stat == 'median':
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanmedian(raster_layerstack, axis=2)
        return
    # Calculate the mean statistical
    if stat == 'mean':
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanmean(raster_layerstack, axis=2)
        return
    # Calculate the standard deviation
    if stat == 'std':
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanstd(raster_layerstack, axis=2)
        return
    # Calculate the valid data
    if stat == 'valid_data':
        # calculate the number of valid data used in statistics products in percentage (0-100%),
        # this count the valid data (no nans) across the layers (time axis)
        num_layers = len(raster_stack)
        output_array[np.ix_(y_chunk, x_chunk)] = \
            (num_layers - np.isnan(raster_layerstack).sum(axis=2)) * 100 / num_layers
        return
    # Calculate the signal-to-noise ratio
    if stat == 'snr':
        # this signal-to-noise ratio defined as the mean divided by the standard deviation.
        m = np.nanmean(raster_layerstack, axis=2)
        sd = np.nanstd(raster_layerstack, axis=2, ddof=0)
        output_array[np.ix_(y_chunk, x_chunk)] = np.where(sd == 0, 0, m / sd)
        return
    # Calculate the coefficient of variation
    if stat == 'coeff_var':
        # the ratio of the biased standard deviation to the mean
        output_array[np.ix_(y_chunk, x_chunk)] = \
            variation(raster_layerstack, axis=2, nan_policy='omit')
        return
    # Calculate the Pearson's correlation coefficient
    if stat == 'pearson_corr':
        # https://github.com/scipy/scipy/blob/v0.14.0/scipy/stats/stats.py#L2392

        # get the numpy 3rd dimension array stack of the bands in chunks for previous file
        prev_raster_layerstack = np.dstack([band[np.ix_(y_chunk, x_chunk)] for band in prev_raster_stack])

        # raster_stack and prev_raster_layerstack should have same length in all axis
        if raster_layerstack.shape != prev_raster_layerstack.shape:
            z_rs = raster_layerstack.shape[2]
            z_prs = prev_raster_layerstack.shape[2]

            if z_rs > z_prs:
                raster_layerstack = np.delete(raster_layerstack, np.s_[z_prs - z_rs:], 2)
            if z_prs > z_rs:
                prev_raster_layerstack = np.delete(prev_raster_layerstack, np.s_[z_rs - z_prs:], 2)

        # propagate the nan values across the pair values in the same position for the
        # two raster in both directions
        mask1 = np.isnan(raster_layerstack)
        mask2 = np.isnan(prev_raster_layerstack)
        combined_mask = mask1 | mask2
        raster_layerstack = np.where(combined_mask, np.nan, raster_layerstack)
        prev_raster_layerstack = np.where(combined_mask, np.nan, prev_raster_layerstack)
        del mask1, mask2, combined_mask

        mean_rs = np.nanmean(raster_layerstack, axis=2, keepdims=True)
        mean_prs = np.nanmean(prev_raster_layerstack, axis=2, keepdims=True)
        m_rs = np.nan_to_num(raster_layerstack - mean_rs)
        m_prs = np.nan_to_num(prev_raster_layerstack - mean_prs)
        r_num = np.add.reduce(m_rs * m_prs, axis=2)
        r_den = np.sqrt(ss(m_rs, axis=2) * ss(m_prs, axis=2))
        r = r_num / r_den

        # write the chunk result r coefficient -1 to 1
        output_array[np.ix_(y_chunk, x_chunk)] = r


def multiprocess_statistic(stat, in_file, raster_stack, outfile, prev_raster_stack=None, number_of_processes=4, tmp_dir=None):
    """Calculate the statistics in multiprocess with chunks of x and y
    """
    # get the projection information
    no_data_value, x_size, y_size, geo_trans, projection, data_type = get_geo_info(in_file)

    # calculate the number of chunks for X
    n_chunks = max(1, ceil(x_size / max(number_of_processes, number_of_processes * floor(x_size / 1000))))
    # divide the rows in n_chunks to process matrix in multiprocess (multi-rows)
    x_chunks = [range(x_size)[i:i + n_chunks] for i in range(0, x_size, n_chunks)]

    # calculate the number of chunks for Y (same len to x_chunks)
    len_chunks = len(x_chunks)
    n_chunks = ceil(y_size / len_chunks)
    # divide the rows in n_chunks to process matrix in multiprocess (multi-rows)
    y_chunks = [range(y_size)[i:i + n_chunks] for i in range(0, y_size, n_chunks)]

    # Pre-allocate a writeable shared memory map as a container for the
    # results of the parallel computation
    tmp_folder = tempfile.mkdtemp(dir=tmp_dir)
    output_file_memmap = os.path.join(tmp_folder, 'output_array')
    output_array = np.memmap(output_file_memmap, dtype=raster_stack[0].dtype,
                             shape=raster_stack[0].shape, mode='w+')

    # make statistics in parallel processes with joblib + memmap
    Parallel(n_jobs=number_of_processes) \
        (delayed(statistic)(stat, raster_stack, output_array, x_chunk, y_chunk, prev_raster_stack)
         for x_chunk, y_chunk in product(x_chunks, y_chunks))

    # define the default output type format
    output_type = gdal.GDT_Float32
    if stat in ['median', 'mean']:
        output_type = gdal.GDT_UInt16

    #### create the output geo tif
    # Set up the GTiff driver
    driver = gdal.GetDriverByName('GTiff')

    new_dataset = driver.Create(outfile, x_size, y_size, 1, output_type,
                                ["COMPRESS=LZW", "PREDICTOR=2", "TILED=YES"])
    # the '1' is for band 1
    new_dataset.SetGeoTransform(geo_trans)
    new_dataset.SetProjection(projection.ExportToWkt())
    # Write the array
    new_dataset.GetRasterBand(1).WriteArray(output_array)
    new_dataset.GetRasterBand(1).SetNoDataValue(np.nan)

    # clean
    del output_array
    shutil.rmtree(tmp_folder)
