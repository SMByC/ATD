#!/usr/bin/env bash
#
#  (c) Copyright SMBYC - IDEAM 2014-2016
#  Authors: Xavier Corredor Llano
#  Email: xcorredorl at ideam.gov.co

########################
# Blended for trimonthly

bands1=79
bands2=88

for file1 in p3_mosaic1/*.tif
do
    echo $file1
    mkdir blended_tmp

    for band in $(seq 1 $bands1); do
        gdal_translate -b ${band} $file1 blended_tmp/${band}.tif
    done

    file2=$(echo $file1 | sed 's/p3_mosaic1/p3_mosaic2/g')
    for band in $(seq 1 $bands2); do
        gdal_translate -b ${band} $file2 blended_tmp/$((bands1+band)).tif
    done

    file_out=$(echo $file1 | sed 's/p3_mosaic1/p3_mosaic/g')
    gdal_merge.py -a_nodata '-28672' -co COMPRESS=LZW -co PREDICTOR=2 \
      -co TILED=YES -co BIGTIFF=YES -o ${file_out} -separate blended_tmp/*.tif

    rm -rf blended_tmp
done

########################
# Blended for semester based on trimonthly

## extract terra
bands1=79
bands2=88

for file1 in p3_mosaic1/*.tif
do
    echo ${file1}

    file1_name=$(echo ${file1} | sed 's|p3_mosaic1/||g')
    file1_name=$(echo ${file1_name} | sed 's|.tif||g')
    dir=blended_terra/${file1_name}
    mkdir -p ${dir}

    for band in $(seq 1 ${bands1}); do
        gdal_translate -b ${band} ${file1} ${dir}/${band}_1t.tif
    done

    file2=$(echo ${file1} | sed 's/p3_mosaic1/p3_mosaic2/g')
    for band in $(seq 1 ${bands2}); do
        gdal_translate -b ${band} ${file2} ${dir}/$((bands1+band))_1t.tif
    done

done

## extract aqua
bands1=88
bands2=81

for file1 in p3_mosaic1/*.tif
do
    echo ${file1}

    file1_name=$(echo ${file1} | sed 's|p3_mosaic1/||g')
    file1_name=$(echo ${file1_name} | sed 's|.tif||g')
    dir=blended_aqua/${file1_name}
    mkdir -p ${dir}

    for band in $(seq 1 ${bands1}); do
        gdal_translate -b ${band} ${file1} ${dir}/${band}_2a.tif
    done

    file2=$(echo ${file1} | sed 's/p3_mosaic1/p3_mosaic2/g')
    for band in $(seq 1 ${bands2}); do
        gdal_translate -b ${band} ${file2} ${dir}/$((bands1+band))_2a.tif
    done

done

## blended in a layerstack
# after extract you need to merge files terra and aqua

for dir in `ls -d *`
do
    cd ${dir}
    file_out=${dir}.tif
    gdal_merge.py -a_nodata '-28672' -co COMPRESS=LZW -co PREDICTOR=2 \
      -co TILED=YES -co BIGTIFF=YES -o ${file_out} -separate `ls -v`
    cd ..
done
