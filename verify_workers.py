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
import argparse
import logging
import multiprocessing
import os
import sys
import subprocess
import peewee

# Logging settings
_logger = logging.getLogger()
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG

# Create database
#
# root@mariadb01 ~ # mysql -uroot -p
# MariaDB [(none)]> CREATE DATABASE verify_workers CHARACTER SET utf8 COLLATE utf8_bin;
# MariaDB [(none)]> GRANT ALL PRIVILEGES ON verify_workers.* TO
# 	verify_workers@'%' IDENTIFIED BY 'wS7BHFEcR5q7BSPmTr7C';

# Database settings
# MariaDB/Galera
DB_HOST = 'galera01.prd.dream.upd.edu.ph'
DB_PORT = '3307'
DB_NAME = 'verify_workers'
DB_USER = 'verify_workers'
DB_PASS = 'wS7BHFEcR5q7BSPmTr7C'
# Sqlite
DB_FILE = 'verify_workers.db'

# Multiprocessing settings
_CPU_USAGE = .5

# Define database models
mysql_db = peewee.MySQLDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(peewee.Model):

    class Meta:
        database = mysql_db


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


def _parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('update_dir_path')

    parser.add_argument('action', choices=['start_worker'])

    args = parser.parse_args()
    return args


def _setup_logging(args):

    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d)\t: %(message)s')

    # Check verbosity for console
    if args.verbose:
        global _CONS_LOG_LEVEL
        _CONS_LOG_LEVEL = logging.DEBUG

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Setup file logging
    LOG_FILE = os.path.splitext(__file__)[0] + '.log'
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel(_FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    _logger.addHandler(fh)


def _update(dir_path):

    # Check if directory path exists
    if not os.path.isdir(dir_path):
        _logger.error("%s doesn't exist! Exiting.", dir_path)
        exit(1)

    # Get server
    file_server = _get_file_server(args.update_dir_path)
    _logger.info('file_server: %s', file_server)

    # Traverse directories
    for root, dirs, files in os.walk(dir_path):

        # Ignore hidden dirs
        dirs[:] = sorted([d for d in dirs if not d[0] == '.'])

        if os.path.isdir(root):
            # Assuming, first two directories of the path is always the mount
            # path
            path_tokens = root.split(os.sep)
            _logger.debug('path_tokens: %s', path_tokens)

            dp = os.sep.join(path_tokens[3:])
            _logger.info('%s', dp)

            # Add dir path as job
            Job.get_or_create(dir_path=dp,
                              file_server=file_server)


def _get_file_server(dir_path):
    mount_out = subprocess.check_output(['mount'])
    for l in mount_out.split('\n')[:-1]:
        tokens = l.split(' on ')
        server_path = tokens[0]
        tokens2 = tokens[1].split(' type ')
        mount_path = tokens2[0]
        # Find mount path while ignoring root
        if mount_path != '/' and mount_path in dir_path:
            # _logger.info('%s: %s', server_path, mount_path)
            # Assuming NFS mount, only get hostname (remove domain)
            server = server_path.split(':')[0].split('.')[0]
            return server


if __name__ == "__main__":

    # Parge arguments
    args = _parse_arguments()

    # Setup logging
    _setup_logging(args)
    _logger.debug('args: %s', args)

    # Connect to database
    mysql_db.connect()
    mysql_db.create_tables([Job, Result], True)

    # Setup pool
    pool = multiprocessing.Pool(processes=int(multiprocessing.cpu_count() *
                                              _CPU_USAGE))

    if 'update_dir_path' in args:
        _logger.info('Update! %s', args.update_dir_path)
        _update(args.update_dir_path)
    elif args.action == 'start_worker':
        pass

    # Close pool
    pool.close()
    pool.join()
