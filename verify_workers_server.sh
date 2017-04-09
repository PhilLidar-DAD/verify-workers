#!/bin/bash

# Activate virtualenv
source /home/datamanager@ad.dream.upd.edu.ph/.virtualenvs/verify-workers/bin/activate

# Change dir
pushd /srv/scripts/sysad-tools/verify-workers

LOG="verify_workers_server.log"
PYTHON_CMD="python -u verify_workers.py"

# Update dirs in db
$PYTHON_CMD update /mnt/pmsat-nas_geostorage/DPC/ARC/ &>$LOG
$PYTHON_CMD update /mnt/maria_geostorage/DPC/LMS/ &>>$LOG
$PYTHON_CMD update /mnt/pmsat-nas_geostorage/DPC/TERRA/ &>>$LOG
$PYTHON_CMD update /mnt/FTP/ &>>$LOG

# Report results
$PYTHON_CMD upload results &>>$LOG

# Return to orig dir
popd
