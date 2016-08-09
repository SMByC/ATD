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


def dump_chunk(x_chunk, y_chunk, in_file, tmp_folder):
    # Open the original file in chunks
    dataset = gdal.Open(in_file, gdal.GA_ReadOnly)
    num_layers = dataset.RasterCount
    # get the numpy 3rd dimension array stack of the bands in chunks (x_chunk and y_chunk)
    raster_layerstack_chunk = np.dstack(
        [dataset.GetRasterBand(band).ReadAsArray(x_chunk[0], y_chunk[0], len(x_chunk), len(y_chunk))
         for band in range(1, num_layers + 1)])
    # raster_band[raster_band == 0] = np.nan
    raster_layerstack_chunk = raster_layerstack_chunk.astype(np.float32)
    # convert the no data value to NaN
    no_data_value = dataset.GetRasterBand(1).GetNoDataValue()
    raster_layerstack_chunk[raster_layerstack_chunk == no_data_value] = np.nan

    # dumb
    file_dump = os.path.join(tmp_folder, "x({0}-{1})_y({2}-{3})".format(x_chunk[0], x_chunk[-1],
                                                                        y_chunk[0], y_chunk[-1]))
    dump(raster_layerstack_chunk, file_dump, compress=0)  # compress=('lzma', 3)
    return file_dump, x_chunk, y_chunk


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
                    in_file = os.path.join(root, file)
                    # get the projection information
                    no_data_value, x_rsize, y_rsize, geo_trans, projection, data_type = get_geo_info(in_file)
                    # calculate the number of chunks for X
                    x_size = max(1, ceil(x_rsize / max(config_run.number_of_processes, config_run.number_of_processes
                                                       * floor(x_rsize / 3000))))
                    # divide the rows in n_chunks to process matrix in multiprocess (multi-rows)
                    x_chunks = [range(x_rsize)[i:i + x_size] for i in range(0, x_rsize, x_size)]

                    # calculate the number of chunks for Y (same len to x_chunks)
                    len_chunks = len(x_chunks)
                    y_size = ceil(y_rsize / len_chunks)
                    # divide the rows in n_chunks to process matrix in multiprocess (multi-rows)
                    y_chunks = [range(y_rsize)[i:i + y_size] for i in range(0, y_rsize, y_size)]

                    ##############
                    print('Dump and load the file to process to disk cache: ', end='', flush=True)
                    # define temp dir and memmap raster to save
                    tmp_folder = tempfile.mkdtemp(dir=config_run.tmp_dir)

                    # dump the input file in chunks in parallel process
                    layerstack_chunks = Parallel(n_jobs=config_run.number_of_processes) \
                        (delayed(dump_chunk)(x_chunk, y_chunk, in_file, tmp_folder)
                         for x_chunk, y_chunk in product(x_chunks, y_chunks))
                    print('OK')

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
                        multiprocess_statistic('median', in_file, layerstack_chunks, out_file, None,
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
                        multiprocess_statistic('mean', in_file, layerstack_chunks, out_file, None,
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

                    out_file = os.path.join(std_dir, os.path.splitext(file)[0] + '_std_x100.tif')

                    try:
                        multiprocess_statistic('std', in_file, layerstack_chunks, out_file, None,
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
                        multiprocess_statistic('valid_data', in_file, layerstack_chunks, out_file, None,
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

                    out_file = os.path.join(snr_dir, os.path.splitext(file)[0] + '_snr_x100.tif')

                    try:
                        multiprocess_statistic('snr', in_file, layerstack_chunks, out_file, None,
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

                    out_file = os.path.join(coeff_var_dir, os.path.splitext(file)[0] + '_coeff_var_x1000.tif')

                    try:
                        multiprocess_statistic('coeff_var', in_file, layerstack_chunks, out_file, None,
                                               config_run.number_of_processes, config_run.tmp_dir)
                        msg = 'OK'
                        config_run.process_logfile.write(msg + '\n')
                        print(msg, flush=True)
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

                        # Pearson's correlation coefficient directory
                        pearson_corr_dir = os.path.join(dir_process, 'pearson_corr')
                        if not os.path.isdir(pearson_corr_dir):
                            os.makedirs(pearson_corr_dir)

                        out_file = os.path.join(pearson_corr_dir, os.path.splitext(file)[0] + '_pearson_corr_x1000.tif')

                        #######
                        # Open and load the previous rundir of mosaic file
                        # get array of the previous mean file
                        print('Dump and load the previous file to process to disk cache: ', end='', flush=True)
                        prev_in_file = os.path.join(previous_p3_mosaic_dir,
                                                    os.path.basename(out_file).split('_pearson_corr_x1000.tif')[0] + '.tif')
                        # define temp dir and memmap raster to save
                        prev_tmp_folder = tempfile.mkdtemp(dir=config_run.tmp_dir)

                        # dump the previous run input file in chunks in parallel process
                        prev_layerstack_chunks = Parallel(n_jobs=config_run.number_of_processes) \
                            (delayed(dump_chunk)(x_chunk, y_chunk, prev_in_file, prev_tmp_folder)
                             for x_chunk, y_chunk in product(x_chunks, y_chunks))
                        print('OK')

                        msg = 'Calculating the Pearson\'s correlation coefficient for {0}: '.format(file)
                        config_run.process_logfile.write(msg)
                        config_run.process_logfile.flush()
                        print(msg, end='', flush=True)

                        try:
                            multiprocess_statistic('pearson_corr', in_file, layerstack_chunks, out_file, prev_layerstack_chunks,
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
                        del prev_layerstack_chunks
                        shutil.rmtree(prev_tmp_folder)

                    # clean
                    del layerstack_chunks
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


def clean_nans_keepdims(z):
    """Clean all nans but keeps dimension for 1D array

    Example:
        input  [ nan   5.  nan  nan  nan   2.   3.]
        output [  5.   2.   3.  nan  nan  nan  nan]
    """
    z_clean = z[~np.isnan(z)]
    base = np.empty((len(z) - len(z_clean)))
    base.fill(np.nan)
    return np.concatenate((z_clean, base))


def statistic(stat, layerstack_chunk, output_array, x_chunk, y_chunk, prev_layerstack_chunks=None):

    # get the numpy 3rd dimension array stack of the bands in chunks (x_chunk and y_chunk)
    layerstack_chunk = load(layerstack_chunk, mmap_mode='r')

    # call built in numpy statistical functions, with a specified axis. if
    # axis=2 means it will calculate along the 'depth' axis, per pixel.
    # with the return being n by m, the shape of each band.
    #
    # Calculate the median statistical
    if stat == 'median':
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanmedian(layerstack_chunk, axis=2)
        return
    # Calculate the mean statistical
    if stat == 'mean':
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanmean(layerstack_chunk, axis=2)
        return
    # Calculate the standard deviation (the result is integer  x10000)
    if stat == 'std':
        layerstack_chunk = np.array(layerstack_chunk)  # TODO: delete when memmap work with nanstd
        output_array[np.ix_(y_chunk, x_chunk)] = np.nanstd(layerstack_chunk, axis=2)
        return
    # Calculate the valid data
    if stat == 'valid_data':
        # calculate the number of valid data used in statistics products in percentage (0-100%),
        # this count the valid data (no nans) across the layers (time axis)
        num_layers = layerstack_chunk.shape[2]
        output_array[np.ix_(y_chunk, x_chunk)] = \
            (num_layers - np.isnan(layerstack_chunk).sum(axis=2)) * 100 / num_layers
        return
    # Calculate the signal-to-noise ratio (the result is integer  x10000)
    if stat == 'snr':
        # this signal-to-noise ratio defined as the mean divided by the standard deviation.
        layerstack_chunk = np.array(layerstack_chunk)  # TODO: delete when memmap work with nanstd
        m = np.nanmean(layerstack_chunk, axis=2)
        sd = np.nanstd(layerstack_chunk, axis=2, ddof=0)
        output_array[np.ix_(y_chunk, x_chunk)] = np.where(sd == 0, 0, m / sd)
        return
    # Calculate the coefficient of variation (the result is integer  x10000)
    if stat == 'coeff_var':
        # the ratio of the biased standard deviation to the mean
        output_array[np.ix_(y_chunk, x_chunk)] = \
            variation(layerstack_chunk, axis=2, nan_policy='omit')
        return
    # Calculate the Pearson's correlation coefficient (the result is integer  x10000)
    if stat == 'pearson_corr':
        # https://github.com/scipy/scipy/blob/v0.14.0/scipy/stats/stats.py#L2392

        # get the respective chunk file based on x_chunk and y_chunk of layerstack_chunk
        prev_layerstack_chunk = [item[0] for item in prev_layerstack_chunks if item[1:3] == (x_chunk, y_chunk)][0]
        # get the numpy 3rd dimension array stack of the bands in chunks for previous file
        prev_layerstack_chunk = load(prev_layerstack_chunk, mmap_mode='r')

        # convert to numpy array
        layerstack_chunk = np.array(layerstack_chunk)
        prev_layerstack_chunk = np.array(prev_layerstack_chunk)

        # clean all nans but keeps dimension for Z axis (time)
        #layerstack_chunk = np.apply_along_axis(clean_nans_keepdims, 2, layerstack_chunk)
        for x, y in product(range(layerstack_chunk.shape[0]), range(layerstack_chunk.shape[1])):
            layerstack_chunk[x][y] = clean_nans_keepdims(layerstack_chunk[x][y])
        #prev_layerstack_chunk = np.apply_along_axis(clean_nans_keepdims, 2, prev_layerstack_chunk)
        for x, y in product(range(prev_layerstack_chunk.shape[0]), range(prev_layerstack_chunk.shape[1])):
            prev_layerstack_chunk[x][y] = clean_nans_keepdims(prev_layerstack_chunk[x][y])

        # layerstack_chunks and prev_layerstack_chunk should have same length in all axis
        if layerstack_chunk.shape != prev_layerstack_chunk.shape:
            z_rs = layerstack_chunk.shape[2]
            z_prs = prev_layerstack_chunk.shape[2]

            if z_rs > z_prs:
                layerstack_chunk = np.delete(layerstack_chunk, np.s_[z_prs - z_rs:], 2)
            if z_prs > z_rs:
                prev_layerstack_chunk = np.delete(prev_layerstack_chunk, np.s_[z_rs - z_prs:], 2)

        # propagate the nan values across the pair values in the same position for the
        # two raster in both directions
        mask1 = np.isnan(layerstack_chunk)
        mask2 = np.isnan(prev_layerstack_chunk)
        combined_mask = mask1 | mask2
        layerstack_chunk = np.where(combined_mask, np.nan, layerstack_chunk)
        prev_layerstack_chunk = np.where(combined_mask, np.nan, prev_layerstack_chunk)
        del mask1, mask2, combined_mask

        mean_rs = np.nanmean(layerstack_chunk, axis=2, keepdims=True)
        mean_prs = np.nanmean(prev_layerstack_chunk, axis=2, keepdims=True)
        m_rs = np.nan_to_num(layerstack_chunk - mean_rs)
        m_prs = np.nan_to_num(prev_layerstack_chunk - mean_prs)
        r_num = np.add.reduce(m_rs * m_prs, axis=2)
        r_den = np.sqrt(ss(m_rs, axis=2) * ss(m_prs, axis=2))
        r = r_num / r_den

        # write the chunk result r coefficient -1 to 1
        output_array[np.ix_(y_chunk, x_chunk)] = r


def multiprocess_statistic(stat, in_file, layerstack_chunks, out_file, prev_layerstack_chunks=None,
                           number_of_processes=os.cpu_count()-2, tmp_dir=None):
    """Calculate the statistics in multiprocess with chunks of x and y
    """
    # get the projection information
    no_data_value, x_size, y_size, geo_trans, projection, data_type = get_geo_info(in_file)

    # Pre-allocate a writeable shared memory map as a container for the
    # results of the parallel computation
    tmp_folder = tempfile.mkdtemp(dir=tmp_dir)
    output_file_memmap = os.path.join(tmp_folder, 'output_array')
    output_array = np.memmap(output_file_memmap, dtype='float32',
                             shape=(y_size, x_size), mode='w+')

    # make statistics in parallel processes with joblib + memmap
    Parallel(n_jobs=number_of_processes) \
        (delayed(statistic)(stat, layerstack_chunk, output_array, x_chunk, y_chunk, prev_layerstack_chunks)
         for layerstack_chunk, x_chunk, y_chunk in layerstack_chunks)

    # convert these statistics to integer values and multiply x10000
    # for keep 4 decimal precision. Int16 range is -32768 to 32767
    if stat in [ 'coeff_var', 'pearson_corr']:
        output_array = np.memmap.dot(output_array, 1000)
        output_type = gdal.GDT_Int16
    # convert snr statistic to integer values and multiply x100
    # Int16 range is -32768 to 32767
    if stat in ['std', 'snr']:
        output_array = np.memmap.dot(output_array, 100)
        output_type = gdal.GDT_Int16
    # define the default output type format.
    # UInt16 range is 0 to 65535
    if stat in ['median', 'mean', 'valid_data']:
        output_type = gdal.GDT_UInt16

    #### create the output geo tif
    # Set up the GTiff driver
    driver = gdal.GetDriverByName('GTiff')

    new_dataset = driver.Create(out_file, x_size, y_size, 1, output_type,
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
