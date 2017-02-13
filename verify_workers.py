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
DB_HOST = 'galera01.prd.dream.upd.edu.ph'
DB_NAME = 'verify_workers'
DB_USER = 'verify_workers'
DB_PASS = 'wS7BHFEcR5q7BSPmTr7C'


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('dir_path')

    args = parser.parse_args()
    return args


def _setup_logging(args):

    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter(
        '[%(asctime)s] (%(levelname)s) : %(message)s')

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


if __name__ == "__main__":

    # Parge arguments
    args = parse_arguments()
    pprint(args)

    # Setup logging
    _setup_logging(args)
