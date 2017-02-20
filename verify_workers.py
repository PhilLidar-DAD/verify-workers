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
from models import *
from pprint import pprint, pformat
from settings import *
import argparse
import distutils
import json
import logging
import os
import random
import subprocess
import sys
import threading
import time

# Logging settings
logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.DEBUG


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('update_dir_path')

    parser_start = subparsers.add_parser('start')
    parser_start.add_argument('start_target', choices=['workers'])

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


@MYSQL_DB.atomic()
def update(dir_path):

    # Connect to database
    logger.info('Connecting to database...')
    MYSQL_DB.connect()
    MYSQL_DB.create_tables([Job, Result], True)

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

            # Remove trailing os separator
            if dp[-1] == os.sep:
                dp = dp[:-1]
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
        logger.debug('FILE_SERVERS: %s', FILE_SERVERS)

        a_ok = True
        for f, v in FILE_SERVERS.viewitems():
            if 'status' not in v or v['status'] != 'OK':
                a_ok = False
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


def start_worker(worker_id):

    # Delay start by (worker id)*5 seconds
    delay = worker_id * 5
    logger.info('[Worker-%s] Delay start for %secs...', worker_id, delay)
    time.sleep(delay)

    # Connect to database
    logger.info('[Worker-%s] Connecting to database...', worker_id)
    MYSQL_DB.connect()
    # MYSQL_DB.create_tables([Job, Result], True)

    # Get directory to verify from db
    while True:
        try:
            job = Job.get((Job.status == None) | (
                (Job.status == 0) & (Job.work_expiry < datetime.now())))
            logger.info('[Worker-%s] Found job: %s:%s', worker_id,
                        job.file_server, job.dir_path)
            verify_dir(worker_id, job)
        except Exception:
            logger.exception('[Worker-%s] Error running job!', worker_id)
        # Sleep for 5mins
        logger.info('[Worker-%s] Sleeping for 5mins...', worker_id)
        time.sleep(60 * 5)


# @MYSQL_DB.atomic()
def verify_dir(worker_id, job):

    with MYSQL_DB.atomic() as txn:
        # Set working status
        job.status = 0
        job.work_expiry = datetime.now() + timedelta(hours=2)  # set time limit to 2hrs
        job.save()
        logger.info('[Worker-%s] %s:%s Verifying...', worker_id, job.file_server,
                    job.dir_path)

    # Get local dir path
    dir_path = os.path.abspath(os.path.join(
        FILE_SERVERS[job.file_server]['local'], job.dir_path))
    logger.info('[Worker-%s] Local directory: %s', worker_id, dir_path)
    if not os.path.isdir(dir_path):
        logger.error("[Worker-%s] %s doesn't exist! Exiting.", worker_id,
                     dir_path)
        exit(1)

    # Get file list
    file_list = {}
    for f in os.listdir(dir_path):
        fp = os.path.join(dir_path, f)
        if os.path.isfile(fp):
            file_list[fp] = None
    logger.debug('[Worker-%s] file_list:\n%s', worker_id,
                 pformat(file_list, width=40))

    # Verify files
    # results = pool.map_async(verify_file, file_list)
    # pprint(results.wait())
    # pprint(results.get())
    # exit(1)

    # for r in results.get():

    for fp in file_list.viewkeys():
        file_list[fp] = verify_file(fp)


    # Add results to db
    with MYSQL_DB.atomic() as txn:
        for fp, v in file_list.viewitems():

            # fp_drv, res = r
            # fp_drv, res = verify_file(fp)
            fp_drv, res = v

            logger.debug('[Worker-%s][%s] %s', worker_id, fp_drv, res)
            if res is not None:
                # Get file path without drive name
                fp = os.path.splitdrive(fp_drv)[1][1:]
                logger.info('[Worker-%s] Saving: %s', worker_id, fp)
                # Add result to db
                db_res, created = Result.get_or_create(file_server=job.file_server,
                                                       file_path=fp, defaults=res)
                logger.debug('%s, %s', db_res, created)
                # If not created, update result in db
                if not created:
                    # logger.info('[Worker-%s] Updating: %s', worker_id, fp)
                    for k, v in res.viewitems():
                        # db_res[k] = v
                        logger.debug('[Worker-%s] Execute: %s', worker_id,
                                     'db_res.' + k + ' = v')
                        exec('db_res.' + k + ' = v')
                    db_res.save()

        # Set done status
        job.status = 1
        job.work_expiry = None
        job.save()
        logger.info('[Worker-%s] %s:%s Done!', worker_id, job.file_server,
                    job.dir_path)


def verify_file(file_path_):

    # Check if file exists
    file_path = os.path.abspath(file_path_)
    if not os.path.isfile(file_path):
        logger.error("[Worker-%s][%s] doesn't exist! Exiting.", worker_id,
                     file_path)
        return file_path, None
    logger.debug('[Worker-%s] file_path: %s', worker_id, file_path)

    # Get file size
    file_size = os.path.getsize(file_path)
    logger.debug('[Worker-%s][%s] file_size: %s', worker_id, file_path,
                 file_size)

    # Check file extension
    file_ext = os.path.splitext(file_path)[1].lower()
    logger.debug('[Worker-%s][%s] file_ext: %s', worker_id, file_path,
                 file_ext)

    # Get checksums and last modified time
    checksum, last_modified = get_checksums(worker_id, file_path)

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
    logger.debug('[Worker-%s][%s] is_processed: %s is_corrupted: %s \
remarks: %s', worker_id, file_path, is_processed, is_corrupted, remarks)

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


def get_checksums(worker_id, file_path):

    dir_path, file_name=os.path.split(file_path)
    # logger.debug('%s: file_name: %s', file_path, repr(file_name))

    # Check if SHA1SUMS file already exists
    checksum=None
    sha1sum_filepath=os.path.join(dir_path, 'SHA1SUMS')
    if os.path.isfile(sha1sum_filepath):
        # Read files from SHA1SUM file that already have checksums
        # logger.debug('[Worker-%s][%s] Reading checksum...', worker_id,
        #     file_path)
        with open(sha1sum_filepath, 'r') as open_file:
            for line in open_file:
                tokens = line.strip().split()
                # Strip wildcard from filename if it exists
                fn = tokens[1]
                if fn.startswith('?'):
                    fn = fn[1:]
                # logger.debug('%s: fn: %s', file_path, repr(fn))
                if fn == file_name:
                    checksum = tokens[0]
    if not checksum:
        # Compute checksum
        # logger.debug('[Worker-%s][%s] Computing checksum...', worker_id,
        #     file_path)
        shasum = subprocess.check_output(['sha1sum', file_path])
        tokens = shasum.strip().split()
        checksum = tokens[0][1:]
    # logger.debug('[Worker-%s][%s] checksum: %s', worker_id, file_path,
    #     checksum)

    # Check if LAST_MODIFIED file already exists
    last_modified = None
    last_modified_filepath = os.path.join(dir_path, 'LAST_MODIFIED')
    if os.path.isfile(last_modified_filepath):
        # logger.debug('[Worker-%s][%s] Reading last modified time...',
        #     worker_id, file_path)
        last_modified_all = json.load(open(last_modified_filepath, 'r'))
        if file_name in last_modified_all:
            last_modified = last_modified_all[file_name]
    if not last_modified:
        # Get last modified time
        # logger.debug('[Worker-%s][%s] Getting last modified time...',
        #     worker_id, file_path)
        last_modified = os.stat(file_path).st_mtime
    # logger.debug('[Worker-%s][%s] last_modified: %s',
    #         worker_id, file_path, last_modified)

    return checksum, last_modified


def verify_raster(file_path):

    outfile = file_path + '.gdalinfo'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['gdalinfo', '-checksum', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err=proc.communicate()
        returncode=proc.returncode
        output={'out': str(out).lower(),
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
        out, err=proc.communicate()
        returncode=proc.returncode
        output={'out': str(out).lower(),
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
        out, err=proc.communicate()
        returncode=proc.returncode
        output={'out': str(out).lower(),
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
        # if 'never classified' in line:
        #     return True

    return False


def verify_archive(file_path):

    outfile = file_path + '.7za'
    # Check if json output file exists
    if not os.path.isfile(outfile):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['7za', 't', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err=proc.communicate()
        returncode=proc.returncode
        output={'out': str(out).lower(),
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

    # multiprocessing.freeze_support()

    # Setup pool
    # logger.info('Setting up pool...')
    # pool = multiprocessing.Pool(processes=int(multiprocessing.cpu_count() *
    #                                           CPU_USAGE))
    # pool = multiprocessing.Pool(1)

    if 'update_dir_path' in args:
        logger.info('Update! %s', args.update_dir_path)
        update(args.update_dir_path)

    elif 'start_target' in args:
        if args.start_target == 'workers':

            # Check if required binaries exist in path
            logger.info('Checking binaries...')
            check_binaries()

            # Check if mapped network drives to the file servers are available
            logger.info('Mapping network drives...')
            map_network_drives()

            # for worker_id in range(1,
            #                        int(multiprocessing.cpu_count() * CPU_USAGE)
            #                        + 1):
            for worker_id in range(1, 3):
                logger.info('Starting worker %s...', worker_id)
                # start_worker(worker_id)
                # Start worker thread
                threading.Thread(target=start_worker, args=(worker_id,)).start()

    # Close pool
    # pool.close()
    # pool.join()
