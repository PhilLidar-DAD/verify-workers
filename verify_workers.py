#!/usr/bin/env python

'''
Copyright (c) 2017, Kenneth Langga (klangga@gmail.com)
All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

from datetime import datetime, timedelta
from pprint import pprint
from settings import *
import argparse
import distutils
import logging
import multiprocessing
import os
import peewee
import subprocess
import sys
import time

# Logging settings
logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.DEBUG

# Create database
#
# root@mariadb01 ~ # mysql -uroot -p
# MariaDB [(none)]> CREATE DATABASE verify_workers CHARACTER SET utf8 COLLATE utf8_bin;
# MariaDB [(none)]> GRANT ALL PRIVILEGES ON verify_workers.* TO
# 	verify_workers@'%' IDENTIFIED BY 'wS7BHFEcR5q7BSPmTr7C';


# Define database models
MYSQL_DB = peewee.MySQLDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(peewee.Model):

    class Meta:
        database = MYSQL_DB


class Job(BaseModel):
    file_server = peewee.CharField()
    dir_path = peewee.CharField()
    status = peewee.IntegerField(choices=[(0, 'Working'),
                                          (1, 'Done')],
                                 null=True)
    work_expiry = peewee.DateTimeField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'dir_path')


class Result(BaseModel):
    file_server = peewee.CharField()
    file_path = peewee.CharField()
    file_ext = peewee.CharField()
    file_size = peewee.BigIntegerField()
    is_processed = peewee.BooleanField()
    is_corrupted = peewee.BooleanField(null=True)
    remarks = peewee.CharField(null=True)
    checksum = peewee.CharField()
    last_modified = peewee.BigIntegerField()
    uploaded = peewee.DateTimeField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'file_path')


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('update_dir_path')

    parser_start = subparsers.add_parser('start')
    parser_start.add_argument('start_target', choices=['worker'])

    args = parser.parse_args()
    return args


def setup_logging(args):

    # Setup logging
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d)\t: %(message)s')

    # Check verbosity for console
    if args.verbose:
        global CONS_LOG_LEVEL
        CONS_LOG_LEVEL = logging.DEBUG

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Setup file logging
    LOG_FILE = os.path.splitext(__file__)[0] + '.log'
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel(FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def update(dir_path):

    # Check if directory path exists
    if not os.path.isdir(dir_path):
        logger.error("%s doesn't exist! Exiting.", dir_path)
        exit(1)

    # Get server
    file_server = get_file_server(args.update_dir_path)
    logger.info('file_server: %s', file_server)

    # Traverse directories
    for root, dirs, files in os.walk(dir_path):

        # Ignore hidden dirs
        dirs[:] = sorted([d for d in dirs if not d[0] == '.'])

        if os.path.isdir(root):
            # Assuming, first two directories of the path is always the mount
            # path
            path_tokens = root.split(os.sep)
            logger.debug('path_tokens: %s', path_tokens)

            dp = os.sep.join(path_tokens[3:])
            logger.info('%s', dp)

            # Add dir path as job
            job, created = Job.get_or_create(dir_path=dp,
                                             file_server=file_server)


def get_file_server(dir_path):
    mount_out = subprocess.check_output(['mount'])
    for line in mount_out.split('\n'):
        l = line.strip()
        if l:
            tokens = l.split(' on ')
            server_path = tokens[0]
            tokens2 = tokens[1].split(' type ')
            mount_path = tokens2[0]
            # Find mount path while ignoring root
            if mount_path != '/' and mount_path in dir_path:
                # logger.info('%s: %s', server_path, mount_path)
                # Assuming NFS mount, only get hostname (remove domain)
                server = server_path.split(':')[0].split('.')[0]
                return server


def start_worker():

    # Check if required binaries exist in path
    logger.info('Checking binaries...')
    check_binaries()

    # Check if mapped network drives to the file servers are available
    logger.info('Mapping network drives...')
    map_network_drives()

    # Get directory to verify from db
    while True:
        job = Job.get((Job.status == None) |
                      (Job.status == 0 & Job.work_expiry < datetime.now()))
        if job:
            verify_dir(job)
        # Sleep for 5mins
        time.sleep(300)


def check_binaries():

    # Add bin folder to PATH
    os.environ['PATH'] = os.path.abspath(
        'bin') + os.pathsep + os.environ['PATH']
    logger.debug("os.environ['PATH']: %s", os.environ['PATH'])

    # Check if binaries exist
    for bin in BINS:
        if distutils.spawn.find_executable(bin) is None:
            print bin, 'is not in path! Exiting.'
            exit(1)


def map_network_drives():

    def all_ok():
        a_ok = True
        net_use_out = subprocess.check_output(['net', 'use'])
        for line in net_use_out.split('\r\n'):
            l = line.strip()
            if l:
                tokens = l.split()
                if len(tokens) == 3:
                    status = tokens[0]
                    local = tokens[1]
                    remote = tokens[2]

                    for f in FILE_SERVERS.viewkeys():
                        fqdn = f + MAP_DRV_DOMN
                        if fqdn in remote:
                            FILE_SERVERS[f]['status'] = status
                            FILE_SERVERS[f]['local'] = local
                            if status != 'OK':
                                logger.debug('Remap! %s %s %s', status,
                                             local, remote)
                                a_ok = False
        logger.debug('FILE_SERVERS: %s', FILE_SERVERS)
        return a_ok

    # Get file server list
    global FILE_SERVERS
    FILE_SERVERS = {}
    for j in Job.select(Job.file_server).distinct():
        FILE_SERVERS[j.file_server] = {}

    # Get map network drives status
    if not all_ok():
        logger.info('...')
        # Remap network drives
        for f, v in FILE_SERVERS.viewitems():
            logger.debug('%s: %s', f, v)
            if 'status' not in v or v['status'] != 'OK':
                local = '*'
                if 'local' in v:
                    local = v['local']
                logger.debug('local: %s', local)
                # Map network drive
                fqdn = f + MAP_DRV_DOMN
                net_use_cmd = ('net use ' + local + r' \\' + fqdn +
                               r'\geostorage /u:' + MAP_DRV_USER + ' ' +
                               MAP_DRV_PASS + ' /persistent:yes')
                logger.debug('net_use_cmd: %s', net_use_cmd)
                subprocess.call(net_use_cmd, shell=True)

        # Get status again
        if not all_ok():
            logger.error('Error mapping network drives! Exiting.')
            exit(1)


def verify_dir(job):
    # Set working status
    job.status = 0
    job.work_expiry = datetime.now() + timedelta(hours=1)  # set time limit to 1hr
    job.save()
    logger.info('%s:%s Verifying...', job.file_server, job.dir_path)

    # Get local dir path
    dir_path = os.path.abspath(os.path.join(
        FILE_SERVERS[job.file_server]['local'], job.dir_path))
    logger.info('Local directory: %s', dir_path)
    if not os.path.isdir(dir_path):
        logger.error("%s doesn't exist! Exiting.")
        exit(1)

    # Get file list
    file_list = []
    for f in os.listdir(dir_path):
        fp = os.path.join(dir_path, f)
        if os.path.isfile(fp):
            # file_size = os.path.getsize(fp)
            # file_list[fp] = {'file_size': file_size,
            #                  'file_ext': None,
            #                  'is_corrupted': None,
            #                  'is_processed': False,
            #                  'remarks': ''}
            file_list.append(fp)

    # Verify files
    # results = pool.map_async(verify_file, file_list)
    # for r in results.get():

    for fp in file_list:

        # fp_drv, res = r
        fp_drv, res = verify_file(fp)

        logger.debug('%s: %s', fp_drv, res)
        if res is not None:
            # Get file path without drive name
            fp = os.path.splitdrive(fp_drv)[1]
            logger.info('Saving: %s', fp)
            # Add result to db
            db_res, created = Result.get_or_create(file_server=job.file_server,
                                                   file_path=fp, defaults=res)
            logger.debug('%s, %s', db_res, created)
            # If not created, update result in db
            if not created:
                logger.info('Updating: %s', fp)
                for k, v in res.viewitems():
                    db_res[k] = v
                db_res.save()

    # Set done status
    job.status = 1
    job.work_expiry = None
    job.save()
    logger.info('%s:%s Done!', job.file_server, job.dir_path)


def verify_file(file_path_):

    # Check if file exists
    file_path = os.path.abspath(file_path_)
    if not os.path.isfile(file_path):
        logger.error("%s doesn't exist! Exiting.", file_path)
        return file_path, None
    logger.debug('file_path: %s', file_path)

    # Get file size
    file_size = os.path.getsize(fp)
    logger.debug('%s: file_size: %s', file_path, file_size)

    # Check file extension
    file_ext = os.path.splitext(file_path)[1].lower()
    logger.debug('%s: file_ext: %s', file_path, file_ext)

    # Get last modified time
    last_modified = os.stat(file_path).st_mtime
    logger.debug('%s: last_modified: %s', file_path, last_modified)

    # Compute checksum
    shasum = subprocess.check_output(['sha1sum', file_path])
    tokens = shasum.strip().split()
    checksum = tokens[0]
    logger.debug('%s: checksum: %s', file_path, checksum)

    is_processed = True
    is_corrupted = None
    remarks = ''
    if file_ext in RASTERS:
        is_corrupted, remarks = verify_raster(file_path)
    elif file_ext in VECTORS:
        is_corrupted = verify_vector(file_path)
    elif file_ext in LAS:
        is_corrupted = verify_las(file_path)
    elif file_ext in ARCHIVES:
        is_corrupted = verify_archive(file_path)
    else:
        is_processed = False
    logger.debug('%s: is_processed: %s is_corrupted: %s remarks: %s',
                 file_path, is_processed, is_corrupted, remarks)

    result = {
        'file_ext': file_ext,
        'file_size': file_size,
        'is_processed': is_processed,
        'is_corrupted': is_corrupted,
        'remarks': remarks,
        'checksum': checksum,
        'last_modified': last_modified
    }

    return file_path, result


def verify_raster(file_path):

    outfile = file_path + '.gdalinfo'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['gdalinfo', '-checksum', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True)

    # Load output from json file
    output = json.load(open(outfile, 'r'))
    # Determine if file is corrupted from output
    if output['returncode'] != 0:
        if 'failed to open grid statistics file' in output['out']:
            return None, 'ERROR! Failed to open grid statistics file (see \
gdalinfo -checksum {raster_path} output)'
        else:
            return True, ''

    if 'error' in output['out']:
        return True, ''

    return False, ''


def verify_vector(file_path):

    outfile = file_path + '.ogrinfo'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['ogrinfo', '-al', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True)

    # Load output from json file
    output = json.load(open(outfile, 'r'))
    # Determine if file is corrupted from output
    if output['returncode'] != 0:
        return True

    if 'ogrfeature' not in output['out']:
        return True

    find_geom = False
    for l in output['out'].split('\n'):
        line = l.strip()
        if 'ogrfeature' in line:
            find_geom = True
        if find_geom:
            for g in GEOMS:
                if g in line:
                    return False
    return True


def verify_las(file_path):

    outfile = file_path + '.lasinfo'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['lasinfo', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True)

    # Load output from json file
    output = json.load(open(outfile, 'r'))
    # Determine if file is corrupted from output
    if output['returncode'] != 0:
        return True

    if 'error' in output['out']:
        return True

    # Ignore these warning messages
    ignored = ['points outside of header bounding box',
               'range violates gps week time specified by global encoding bit 0']
    # Parse output for warning messages
    for l in output['out'].split('\n'):
        line = l.strip()
        # Ignore some lines
        ignore_line = False
        for i in ignored:
            if i in line:
                ignore_line = True
                break
        if ignore_line:
            continue

        if 'warning' in line:
            return True
        if 'never classified' in line:
            return True

    return False


def verify_archive(file_path):

    outfile = file_path + '.7za'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['7za', 't', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True)

    # Load output from json file
    output = json.load(open(outfile, 'r'))
    # Determine if file is corrupted from output
    if output['returncode'] != 0:
        return True

    if 'error' in output['out']:
        return True

    return False


if __name__ == "__main__":

    # Parge arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(args)
    logger.debug('args: %s', args)

    # Connect to database
    logger.info('Connecting to database...')
    MYSQL_DB.connect()
    MYSQL_DB.create_tables([Job, Result], True)

    # Setup pool
    logger.info('Setting up pool...')
    pool = multiprocessing.Pool(processes=int(multiprocessing.cpu_count() *
                                              CPU_USAGE))

    if 'update_dir_path' in args:
        logger.info('Update! %s', args.update_dir_path)
        update(args.update_dir_path)
    elif 'start_target' in args:
        if args.start_target == 'worker':
            logger.info('Starting worker...')
            start_worker()

    # Close pool
    pool.close()
    pool.join()
