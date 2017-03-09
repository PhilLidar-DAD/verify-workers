#!/bin/bash

# Activate virtualenv
source /home/datamanager@ad.dream.upd.edu.ph/.virtualenvs/verify-workers/bin/activate

# Change dir
pushd /srv/scripts/sysad-tools/verify-workers

# Update dirs in db
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/ARC/
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/TERRA/
./verify_workers.py update /mnt/maria_geostorage/DPC/LMS/

# Report results
./verify_workers.py upload results

# Return to orig dir
popd
