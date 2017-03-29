#!/bin/bash

# Activate virtualenv
source /home/datamanager@ad.dream.upd.edu.ph/.virtualenvs/verify-workers/bin/activate

# Change dir
pushd /srv/scripts/sysad-tools/verify-workers

LOG="verify_workers_server.log"

# Update dirs in db
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/ARC/ &>$LOG
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/TERRA/ &>$LOG
./verify_workers.py update /mnt/maria_geostorage/DPC/LMS/ &>$LOG
./verify_workers.py update /mnt/FTP/ &>$LOG

# Report results
./verify_workers.py upload results &>$LOG

# Return to orig dir
popd
