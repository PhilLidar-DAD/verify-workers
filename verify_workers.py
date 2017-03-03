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
from google_sheet import GoogleSheet
from models import MYSQL_DB, Job, Result
from pprint import pprint, pformat
from settings import *
import argparse
import distutils
import json
import logging
import os
import peewee
import random
import re
import socket
import subprocess
import sys
import threading
import time

# Logging settings
logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.DEBUG
START_EPOCH = datetime(1970, 1, 1)


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('update_dir_path')

    parser_start = subparsers.add_parser('start')
    parser_start.add_argument('start_target', choices=['workers'])

    parser_upload = subparsers.add_parser('upload')
    parser_upload.add_argument('upload_target', choices=['results', 'reset'])

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

    # Connect to database
    logger.info('Connecting to database...')
    MYSQL_DB.connect()
    # with MYSQL_DB.atomic() as txn:
    #     MYSQL_DB.create_tables([Job, Result], True)

    # Check if directory path exists
    if not os.path.isdir(dir_path):
        logger.error("%s doesn't exist! Exiting.", dir_path)
        exit(1)

    # Get server
    file_server = get_file_server(args.update_dir_path)
    logger.info('file_server: %s', file_server)

    # Temporarily set is_dir to False for all dirs in db that starts with
    # prefix
    path_tokens = dir_path.split(os.sep)
    dp_prefix = os.sep.join(path_tokens[3:])
    if dp_prefix[-1] == os.sep:
        dp_prefix = dp_prefix[:-1]
    logger.info('dp_prefix: %s', dp_prefix)
    with MYSQL_DB.atomic() as txn:
        query = (Job
                 .update(is_dir=False)
                 .where(Job.dir_path.startswith(dp_prefix)))
        query.execute()

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
            with MYSQL_DB.atomic() as txn:
                job, created = Job.get_or_create(file_server=file_server,
                                                 dir_path=dp,
                                                 defaults={'is_dir': True})
                # If not created, update result in db
                if not created:
                    job.is_dir = True
                    job.save()

    # Delete all dirs that don't exist anymore
    with MYSQL_DB.atomic() as txn:
        query = (Job
                 .delete()
                 .where((Job.is_dir == False) &
                        (Job.dir_path.startswith(dp_prefix))))
        query.execute()


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

    # Delay start
    delay = random.randint((worker_id - 1) * 5 + 1, worker_id * 5)
    logger.info('[Worker-%s] Delay start for %ssecs...', worker_id, delay)
    time.sleep(delay)

    # Connect to database
    # logger.info('[Worker-%s] Connecting to database...', worker_id)

    # Get directory to verify from db
    while True:
        try:
            MYSQL_DB.connect()
            # job = Job.get((Job.status == None) | (
            #     (Job.status == 0) & (Job.work_expiry < datetime.now())))
            job = (Job
                   .select()
                   .where((Job.status == None) |
                          ((Job.status == 0) &
                           (Job.work_expiry < datetime.now())))
                   .order_by(peewee.fn.Rand())
                   .get())
            logger.info('[Worker-%s] Found job: %s:%s', worker_id,
                        job.file_server, job.dir_path)
            verify_dir(worker_id, job)
        except Exception:
            logger.exception('[Worker-%s] Error running job!', worker_id)
        finally:
            if not MYSQL_DB.is_closed():
                MYSQL_DB.close()
        # Sleep
        delay = random.randint(1, 5)
        logger.info('[Worker-%s] Sleeping for %ssecs...', worker_id, delay)
        time.sleep(delay)


def verify_dir(worker_id, job):

    with MYSQL_DB.atomic() as txn:
        # Set working status
        job.status = 0
        job.work_expiry = datetime.now() + timedelta(hours=1)  # set time limit to 1hr
        job.save()
        # logger.info('[Worker-%s] Verifying %s:%s...', worker_id, job.file_server,
        #             job.dir_path)

    # Get local dir path
    dir_path = os.path.abspath(os.path.join(
        FILE_SERVERS[job.file_server]['local'], job.dir_path))
    logger.info('[Worker-%s] Local directory: %s', worker_id, dir_path)
    if not os.path.isdir(dir_path):
        logger.error("[Worker-%s] %s doesn't exist! Exiting.", worker_id,
                     dir_path)
        return

    # Get file list
    logger.info('[Worker-%s] Getting file list...', worker_id)
    file_list = {}
    for f in os.listdir(dir_path):
        fp = os.path.join(dir_path, f)
        if os.path.isfile(fp):
            file_list[fp] = None
    logger.debug('[Worker-%s] file_list:\n%s', worker_id,
                 pformat(file_list, width=40))

    # Verify files
    logger.info('[Worker-%s] Verifying files...', worker_id)
    for fp in file_list.viewkeys():
        file_list[fp] = verify_file(worker_id, fp)

    # Save results to db
    logger.info('[Worker-%s] Saving results to db...', worker_id)
    with MYSQL_DB.atomic() as txn:
        for fp, v in file_list.viewitems():

            fp_drv, res = v

            logger.debug('[Worker-%s][%s] %s', worker_id, fp_drv, res)
            if res is not None:
                # Get file path without drive name
                drive, tail = os.path.splitdrive(fp_drv)
                logger.debug('[Worker-%s] %s, %s', worker_id, drive, tail)
                fp = tail[1:]
                logger.debug('[Worker-%s] fp: %s', worker_id, fp)
                # Add result to db
                db_res, created = Result.get_or_create(file_server=job.file_server,
                                                       file_path=fp,
                                                       defaults=res)
                logger.debug('%s, %s', db_res, created)
                # If not created, update result in db
                if not created:
                    for k, v in res.viewitems():
                        # db_res[k] = v
                        # logger.debug('[Worker-%s] Execute: %s', worker_id,
                        #              'db_res.' + k + ' = v')
                        exec('db_res.' + k + ' = v')
                    db_res.save()

        # Set done status
        job.status = 1
        job.work_expiry = None
        job.save()
        logger.info('[Worker-%s] %s:%s Done!', worker_id, job.file_server,
                    job.dir_path)


def verify_file(worker_id, file_path_):

    # Check if file exists
    file_path = os.path.abspath(file_path_)
    if not os.path.isfile(file_path):
        logger.error("[Worker-%s][%s] doesn't exist! Exiting.", worker_id,
                     file_path)
        return file_path, None
    logger.info('[Worker-%s] Verifying: %s', worker_id, file_path)

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

    file_type = None
    is_processed = True
    has_error = None
    remarks = ''
    if file_ext in RASTERS:
        file_type = 'RASTER'
        has_error, remarks = verify_raster(file_path)
    elif file_ext in VECTORS:
        file_type = 'VECTOR'
        has_error, remarks = verify_vector(file_path)
    elif file_ext in LAS:
        file_type = 'LAS/LAZ'
        has_error, remarks = verify_las(file_path)
    elif file_ext in ARCHIVES:
        file_type = 'ARCHIVE'
        has_error, remarks = verify_archive(file_path)
    else:
        is_processed = False
    logger.debug('[Worker-%s][%s] is_processed: %s has_error: %s \
remarks: %s', worker_id, file_path, is_processed, has_error, remarks)

    result = {
        'file_ext': file_ext,
        'file_type': file_type,
        'file_size': file_size,
        'is_processed': is_processed,
        'has_error': has_error,
        'remarks': remarks,
        'checksum': checksum,
        'last_modified': last_modified,
        'processor': processor
    }

    return file_path, result


def get_checksums(worker_id, file_path):

    dir_path, file_name = os.path.split(file_path)

    # Check if SHA1SUMS file already exists
    checksum = None
    sha1sum_filepath = os.path.join(dir_path, 'SHA1SUMS')
    if os.path.isfile(sha1sum_filepath):
        # Read files from SHA1SUM file that already have checksums
        with open(sha1sum_filepath, 'r') as open_file:
            for line in open_file:
                tokens = line.strip().split()
                # Strip wildcard from filename if it exists
                fn = tokens[1]
                if fn.startswith('?'):
                    fn = fn[1:]
                if fn == file_name:
                    checksum = tokens[0]
    if not checksum:
        # Compute checksum
        shasum = subprocess.check_output(['sha1sum', file_path])
        tokens = shasum.strip().split()
        checksum = tokens[0][1:]

    # Check if LAST_MODIFIED file already exists
    last_modified = None
    last_modified_filepath = os.path.join(dir_path, 'LAST_MODIFIED')
    if os.path.isfile(last_modified_filepath):

        last_modified_all = json.load(open(last_modified_filepath, 'r'))
        if file_name in last_modified_all:
            last_modified = last_modified_all[file_name]
    if not last_modified:
        # Get last modified time
        last_modified = os.stat(file_path).st_mtime

    return checksum, last_modified


def verify_raster(file_path):

    outfile = file_path + '.gdalinfo'
    output = None
    # Check if json output file exists
    if os.path.isfile(outfile):
        try:
            # Load output from json file
            output = json.load(open(outfile, 'r'))

            # Reverify if dll wasn't loaded last time
            if "can't load requested dll" in output['out']:
                output = None
        except Exception:
            pass

    if output is None:
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['gdalinfo', '-checksum', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Determine if file is corrupted from output
    remarks_buf = []
    has_error = False

    if output['returncode'] != 0:
        has_error = True
        remarks_buf.append('Error while opening file')

    for l in output['out'].split('\n'):
        line = l.strip()
        if 'failed to open grid statistics file' in line:
            has_error = None
            remarks_buf.append('Failed to open grid statistics file')
            # Ignore other error lines if they appear
            break
        if 'error' in line:
            has_error = True
            remarks_buf.append(line)

    remarks = '\n'.join(remarks_buf)

    return has_error, remarks


def verify_vector(file_path):

    outfile = file_path + '.ogrinfo'
    output = None
    # Check if json output file exists
    if os.path.isfile(outfile):
        try:
            # Load output from json file
            output = json.load(open(outfile, 'r'))

            # Reverify if dll wasn't loaded last time
            if "can't load requested dll" in output['out']:
                output = None
        except Exception:
            pass

    if output is None:
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['ogrinfo', '-al', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Determine if file is corrupted from output
    remarks_buf = []
    has_error = False

    if output['returncode'] != 0:
        has_error = True
        remarks_buf.append('Error while opening file')

    if 'ogrfeature' not in output['out']:
        has_error = True
        remarks_buf.append('ogrfeature not found')

    find_geom = False
    found_geom = False
    for l in output['out'].split('\n'):
        line = l.strip()
        if 'ogrfeature' in line:
            find_geom = True
        if find_geom:
            for g in GEOMS:
                if g in line:
                    found_geom = True
                    break

    if not found_geom:
        has_error = True
        remarks_buf.append('Cannot find geom')

    remarks = '\n'.join(remarks_buf)

    return has_error, remarks


def verify_las(file_path):

    outfile = file_path + '.lasinfo'
    output = None
    # Check if json output file exists
    if os.path.isfile(outfile):
        try:
            # Load output from json file
            output = json.load(open(outfile, 'r'))

            # Reverify if dll wasn't loaded last time
            if "can't load requested dll" in output['out']:
                output = None
        except Exception:
            pass

    if output is None:
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['lasinfo', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Determine if file is corrupted from output
    remarks_buf = []
    has_error = False

    if output['returncode'] != 0:
        has_error = True
        remarks_buf.append('Error while opening file')

    # Ignore these warning messages
    ignored = [r'points outside of header bounding box',
               r'range violates gps week time specified by global encoding bit 0',
               r'for return [0-9]+ real number of points by return \([0-9]+\) is different from header entry \([0-9]+\)']
    # Parse output for warning messages
    for l in output['out'].split('\n'):
        line = l.strip()

        if 'error' in line:
            has_error = True
            remarks_buf.append(line)

        if 'warning' in line:
            remarks_buf.append(line)

            # Check if warning is ignored
            ignore_line = False
            for i in ignored:
                if re.search(i, line):
                    ignore_line = True
                    break
            if not ignore_line:
                has_error = True

    remarks = '\n'.join(remarks_buf)

    return has_error, remarks


def verify_archive(file_path):

    outfile = file_path + '.7za'
    output = None
    # Check if json output file exists
    if os.path.isfile(outfile):
        try:
            # Load output from json file
            output = json.load(open(outfile, 'r'))

            # Reverify if dll wasn't loaded last time
            if "can't load requested dll" in output['out']:
                output = None
        except Exception:
            pass

    if output is None:
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['7za', 't', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Load output from json file
    output = json.load(open(outfile, 'r'))
    # Determine if file is corrupted from output
    remarks_buf = []
    has_error = False

    if output['returncode'] != 0:
        has_error = True
        remarks_buf.append('Error while opening file')

    for l in output['out'].split('\n'):
        line = l.strip()
        if 'error' in line:
            has_error = True
            remarks_buf.append(line)

    remarks = '\n'.join(remarks_buf)

    return has_error, remarks


def simplify_backslashes(p):
    while r'\\' in p:
        p = p.replace(r'\\', '\\')
    return p


def upload_results(spreadsheetId, rangeName, has_error_only=True):

    # Get current values table
    logger.info('Getting current values from Google Sheet...')
    gs = GoogleSheet(spreadsheetId)
    values = gs.get_values(rangeName)

    # Convert values table to dict
    logger.info('Converting values to dict...')
    values_dict = {}
    headers = True
    for row in values:

        # Ignore headers
        if headers:
            headers = False
            continue

        file_server = row[0]  # A
        file_path = simplify_backslashes(row[1])  # B
        file_ext = row[2]  # C
        file_type = row[3]  # D
        file_size = row[4]  # E
        is_processed = row[5]  # F
        has_error = row[6]  # G
        remarks = row[7]  # H
        checksum = row[9]  # J
        last_modified = row[10]  # K
        uploaded = row[11]  # L
        processor = row[12]  # M

        values_dict[(file_server, file_path)] = {
            'file_ext': file_ext,
            'file_type': file_type,
            'file_size': file_size,
            'is_processed': is_processed,
            'has_error': has_error,
            'remarks': remarks,
            'checksum': checksum,
            'last_modified': last_modified,
            'uploaded': uploaded,
            'processor': processor,
        }

    # Get all results
    logger.info('Getting all results from db and updating dict...')
    MYSQL_DB.connect()
    q = Result.select().where(Result.uploaded == None)
    if has_error_only:
        q = Result.select().where((Result.uploaded == None) &
                                  (Result.has_error == True))

    has_new_results = False
    with MYSQL_DB.atomic() as txn:
        for r in q:
            has_new_results = True

            k = (r.file_server, r.file_path)

            if k in values_dict:
                r.uploaded = datetime(2017, 2, 24)
            else:
                r.uploaded = datetime.now()
                values_dict[k] = {}

            values_dict[k]['file_ext'] = r.file_ext
            values_dict[k]['file_type'] = r.file_type
            values_dict[k]['file_size'] = r.file_size
            values_dict[k]['is_processed'] = r.is_processed
            values_dict[k]['has_error'] = r.has_error
            # Limit remarks to 1000 chars
            remarks = r.remarks
            if len(remarks) >= 1000:
                remarks = remarks[:997] + '...'
            values_dict[k]['remarks'] = remarks
            values_dict[k]['checksum'] = r.checksum
            values_dict[k]['last_modified'] = datetime.fromtimestamp(
                r.last_modified).strftime('%Y-%m-%d %H:%M:%S')
            values_dict[k]['processor'] = r.processor

            values_dict[k]['uploaded'] = r.uploaded.strftime('%b %d, %Y')

            r.save()

    if not has_new_results:
        logger.info('No new results! Exiting.')
        return

    # Create new values list
    logger.info('Creating new values list...')
    values = []
    headers = ['file_server',
               'file_path',
               'file_ext',
               'file_type',
               'file_size',
               'is_processed',
               'has_error',
               'remarks',
               'remarks',
               'checksum',
               'last_modified',
               'processor',
               'uploaded']
    values.append(headers)
    for k in sorted(values_dict.keys()):

        row = []

        file_server, file_path = k
        row.append(file_server)
        row.append(file_path)

        row.append(values_dict[k]['file_ext'])
        row.append(values_dict[k]['file_type'])
        row.append(values_dict[k]['file_size'])
        row.append(values_dict[k]['is_processed'])
        row.append(values_dict[k]['has_error'])
        row.append(values_dict[k]['remarks'])
        row.append('')
        row.append(values_dict[k]['checksum'])
        row.append(values_dict[k]['last_modified'])
        row.append(values_dict[k]['processor'])
        row.append(values_dict[k]['uploaded'])

        values.append(row)

    # Update Google Sheeet
    logger.info('Updating Google Sheet...')
    gs.set_values(rangeName, values)

    logger.info('Done!')


def upload_reset():
    logger.info('Connecting to database...')
    MYSQL_DB.connect()
    with MYSQL_DB.atomic() as txn:
        query = Result.update(uploaded=None)
        query.execute()
        logger.info('Done!')

if __name__ == "__main__":

    # Parge arguments
    args = parse_arguments()

    # Setup logging
    setup_logging(args)
    logger.debug('args: %s', args)

    # Get hostname
    processor = socket.gethostname().lower()
    logger.info('Processor: %s', processor)

    if 'update_dir_path' in args:
        logger.info('Update! %s', args.update_dir_path)
        update(args.update_dir_path)

    elif 'start_target' in args and args.start_target == 'workers':

        # Check if required binaries exist in path
        logger.info('Checking binaries...')
        check_binaries()

        # Check if mapped network drives to the file servers are available
        logger.info('Mapping network drives...')
        map_network_drives()

        logger.info('Starting %s workers...', WORKERS)
        for worker_id in range(1, WORKERS + 1):
            logger.info('Starting worker %s...', worker_id)
            try:
                # Start worker thread
                threading.Thread(target=start_worker,
                                 args=(worker_id,)).start()
            except Exception:
                logger.exception('[Worker-%s] Error running worker!',
                                 worker_id)

    elif 'upload_target' in args:

        if args.upload_target == 'results':

            logger.info('Uploading results...')
            upload_results('1j2nNxHfqEFnDC_qFu00ncCM7139gR5OxWMcs5ylXDMQ',
                           'Corrupted list!A:M')

        elif args.upload_target == 'reset':

            logger.info('Resetting upload status...')
            upload_reset()
