#!/usr/bin/env bash

if [ ! -d ${ATD_DIR} ]; then
    mkdir -p $ATD_DIR
fi

cd $ATD_DIR

# check if the project ATD exist with VCS
if [ ! -d ${ATD_DIR}/.hg ]; then
    cd ..
    rm -rf ATD
    hg clone ssh://hg@bitbucket.org/SMBYC/atd ATD
    cd $ATD_DIR
fi

# synchronize changes with repository, update and clean
hg pull
hg update -C
hg status -un|xargs rm 2> /dev/null