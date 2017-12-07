#!/bin/bash
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/ARC/ &>DPC_ARC.log & disown -a
./verify_workers.py update /mnt/maria_geostorage/DPC/LMS/ &>DPC_LMS.log & disown -a
./verify_workers.py update /mnt/pmsat-nas_geostorage/DPC/TERRA/ &>DPC_TERRA.log & disown -a
./verify_workers.py update /mnt/pmsat-nas_geostorage/GISDATA/ &>GISDATA.log & disown -a
./verify_workers.py update /mnt/pmsat-nas_geostorage/GISDATA/ &>GISDATA.log & disown -a
./verify_workers.py update /mnt/pmsat-nas_geostorage/EXCHANGE/ &>EXCHANGE.log & disown -a
./verify_workers.py update /mnt/pmsat-nas_geostorage/DAC/RAWDATA/ &>DAC_RAWDATA.log & disown -a
