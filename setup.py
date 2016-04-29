# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    README = f.read()

setup(
    name='ATD',
    version='0.1',
    description='Automated Process of Early Alert Deforestation',
    long_description=README,
    author='Xavier Corredor Llano, SMBYC-IDEAM',
    author_email='xcorredorl@ideam.gov.co, smbyc@ideam.gov.co',
    url='https://bitbucket.org/SMBYC/atd',
    license='GPLv3',
    packages=find_packages(exclude=('docs',)),
    install_requires=['gdal',
                      'numpy',
                      'python-dateutil'],
    extras_require={
        'statistics process': ["R", "sp (Rlib)", "raster (Rlib)", "rgdal (Rlib)",
                               "spatial (Rlib)", "plyr (Rlib)", "doSNOW (Rlib)"]},
    platforms=['Any'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Scientific/Engineering :: GIS",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Environment :: Console",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"],
)
