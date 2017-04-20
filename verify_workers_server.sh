#!/bin/bash

# Check arguments
case "$1" in
    update|suggest|upload)
        ACTION="$1"
        ;;
    *)
        echo 'Action must be set (update|suggest|upload)! Exiting.'
        exit 1
        ;;
esac

# Activate virtualenv
source /home/datamanager@ad.dream.upd.edu.ph/.virtualenvs/verify-workers/bin/activate

# Change dir
pushd /srv/scripts/sysad-tools/verify-workers

LOG="verify_workers_server.log"
PYTHON_CMD="python -u verify_workers.py"

# Empty log
echo &>$LOG

case $ACTION in
    update)
        # Update dirs in db
        $PYTHON_CMD update /mnt/pmsat-nas_geostorage/DPC/ARC/ &>>$LOG
        $PYTHON_CMD update /mnt/maria_geostorage/DPC/LMS/ &>>$LOG
        $PYTHON_CMD update /mnt/pmsat-nas_geostorage/DPC/TERRA/ &>>$LOG
        $PYTHON_CMD update /mnt/FTP/ &>>$LOG
        ;;
    suggest)
        # Find ftp suggestions
        $PYTHON_CMD start suggest &>>$LOG
        ;;
    upload)
        # Report results
        $PYTHON_CMD upload results &>>$LOG
        ;;
esac

# Return to orig dir
popd
