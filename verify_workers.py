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
    is_corrupted = peewee.BooleanField()
    is_processed = peewee.BooleanField()
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
            Job.get_or_create(dir_path=dp,
                              file_server=file_server)


def get_file_server(dir_path):
    mount_out = subprocess.check_output(['mount'])
    for l in mount_out.split('\n')[:-1]:
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
    check_binaries()

    # Check if mapped network drives to the file servers are available
    map_network_drives()


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

    # Get file server list
    file_servers = [j.file_server
                    for j in Job.select(Job.file_server).distinct()]
    logger.debug('file_servers: %s', file_servers)

if __name__ == "__main__":

    # Parge arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(args)
    logger.debug('args: %s', args)

    # Connect to database
    MYSQL_DB.connect()
    MYSQL_DB.create_tables([Job, Result], True)

    # Setup pool
    pool = multiprocessing.Pool(processes=int(multiprocessing.cpu_count() *
                                              CPU_USAGE))

    if 'update_dir_path' in args:
        logger.info('Update! %s', args.update_dir_path)
        update(args.update_dir_path)
    elif 'start_target' in args:
        if args.start_target == 'worker':
            start_worker()

    # Close pool
    pool.close()
    pool.join()
