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

from __future__ import division
from datetime import datetime, timedelta
from google_sheet import GoogleSheet
from models import *
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


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")

    subparsers = parser.add_subparsers()

    parser_update = subparsers.add_parser('update')
    parser_update.add_argument('update_dir_path')

    parser_start = subparsers.add_parser('start')
    parser_start.add_argument('start_target', choices=['workers', 'migrate',
                                                       'suggest'])

    parser_upload = subparsers.add_parser('upload')
    parser_upload.add_argument('upload_target', choices=['results'])

    parser_extras = subparsers.add_parser('extras')
    parser_extras.add_argument('extras_target', choices=['fixpathsep'])

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


def connect_db():
    # Connect to database
    retry = True
    while retry:
        try:
            MYSQL_DB.connect()
            retry = False
        except Exception:
            delay = random.randint(0, 1000) / 1000.
            # logger.exception(
            #     '[Worker-%s] Error connecting to database! Retrying in %ss...', pid, delay)
            logger.exception(
                'Error connecting to database! Retrying in %ss...', delay)
            retry = True
            time.sleep(delay)


def close_db():
    # Close database
    if not MYSQL_DB.is_closed():
        MYSQL_DB.close()


def trim_mount_path(path):
    path_tokens = path.split(os.sep)
    new_path = os.sep.join(path_tokens[3:])
    # Return None if empty
    if new_path == '':
        return
    # Remove trailing os separator
    if new_path[-1] == os.sep:
        new_path = new_path[:-1]
    return new_path


def update_dir(update_dir_path):

    # Check if directory path exists
    if not os.path.isdir(update_dir_path):
        logger.error("%s doesn't exist! Exiting.", update_dir_path)
        exit(1)

    # Get file server
    file_server = get_file_server(args.update_dir_path)
    logger.info('file_server: %s', file_server)

    # Invalidate all dir_path's in job and all file_path's in result
    logger.info('Connecting to database...')
    connect_db()
    logger.info("Invalidating all dir_path's in job and all file_path's in \
result tables...")
    dp_prefix = trim_mount_path(update_dir_path)
    logger.info('dp_prefix: %s', dp_prefix)
    with MYSQL_DB.atomic() as txn:
        # Job
        if dp_prefix:
            query = (Job
                     .update(is_dir=False)
                     .where(Job.dir_path.startswith(dp_prefix)))
        else:
            # If prefix isn't available, use file server
            query = (Job
                     .update(is_dir=False)
                     .where(Job.file_server == file_server))
        query.execute()
        logger.info('Job done...')
        # Result
        if dp_prefix:
            query = (Result
                     .update(is_file=False)
                     .where(Result.file_path.startswith(dp_prefix)))
        else:
            # If prefix isn't available, use file server
            query = (Result
                     .update(is_file=False)
                     .where(Result.file_server == file_server))
        query.execute()
        logger.info('Result done...')
    close_db()

    # Initialize multiprocessing
    start_time = datetime.now()
    manager = multiprocessing.Manager()
    pool = multiprocessing.Pool(processes=WORKERS)
    dir_paths = manager.Queue()

    # Traverse directories
    counter = 1
    dir_count = 1
    pool.apply_async(update_worker, (file_server, update_dir_path, dir_paths))

    while counter > 0:
        logger.debug('counter: %s', counter)
        dir_path = dir_paths.get()
        if dir_path == 'no-dir':
            # dir has finished processing
            counter -= 1
            # logger.info('counter(-): %s', counter)
        else:
            # a new dir needs to be processed
            counter += 1
            dir_count += 1
            pool.apply_async(update_worker, (file_server, dir_path, dir_paths))
            # logger.info('counter(+): %s', counter)

    pool.close()
    pool.join()
    end_time = datetime.now()
    logger.info('dir_count: %s', dir_count)
    logger.info('Done! (%s)', end_time - start_time)


def ignore_file_dir(name):
    # Hidden files/dirs
    if name.startswith('.'):
        return True
    # Checksums
    if name in ['LAST_MODIFIED', 'SHA1SUMS']:
        return True
    # Output files
    for file_ext in ['.gdalinfo', '.ogrinfo', '.lasinfo', '.7za']:
        if name.endswith(file_ext):
            return True

    return False


def update_worker(file_server, dir_path, dir_paths):
    try:
        # Check if dir path exists
        if os.path.isdir(dir_path):
            # Get process id
            pid = multiprocessing.current_process().pid
            connect_db()
            # Get trimmed dir path
            job_dp = trim_mount_path(dir_path)
            logger.info('[Worker-%s] %s', pid, job_dp)
            # For each content inside the directory
            status_done = True
            for i in sorted(os.listdir(dir_path)):
                # Ignore some files/dirs
                if ignore_file_dir(i):
                    continue
                # Get complete path
                i_path = os.path.join(dir_path, i)
                # Check if directory
                if os.path.isdir(i_path):
                    # Add dir path to queue
                    dir_paths.put(i_path)
                # Check if file
                elif os.path.isfile(i_path):
                    # Get trimmed file path
                    result_fp = trim_mount_path(i_path)
                    # Get result file object from db
                    result = None
                    try:
                        result = (Result
                                  .select()
                                  .where((Result.file_server == file_server)
                                         & (Result.file_path == result_fp))
                                  .get())
                    except Result.DoesNotExist:
                        status_done = False
                    if result:
                        # Get file size
                        file_size = os.path.getsize(i_path)
                        # Get checksums and last modified time
                        checksum, last_modified = get_checksums(i_path,
                                                                skip_checksum=True)
                        # If checksum matches db or file size and last
                        # modified are the same
                        if ((checksum and checksum == result.checksum) or
                                (file_size == result.file_size and
                                    last_modified == result.last_modified)):
                            # Validate file
                            result.is_file = True
                            # Set dir_path and filename if null
                            if not result.dir_path or not result.filename:
                                dirname, filename = os.path.split(result_fp)
                                result.dir_path = dirname
                                result.filename = filename
                            with MYSQL_DB.atomic() as txn:
                                # Save
                                result.save()
                        else:
                            status_done = False
            # Add dir path as job
            if job_dp:
                with MYSQL_DB.atomic() as txn:
                    job, created = Job.get_or_create(file_server=file_server,
                                                     dir_path=job_dp,
                                                     defaults={'is_dir': True})
                    # If not created, update result in db
                    if not created:
                        job.is_dir = True
                        # If there are new files, reset done status
                        if not status_done:
                            job.status = None
                        job.save()
    except Exception:
        logger.exception('Error running update worker! (%s)', dir_path)
    finally:
        close_db()
    dir_paths.put('no-dir')


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
                if 'ftp' in f:
                    net_use_cmd = ('net use ' + local + r' \\' + fqdn +
                                   r'\FTP /u:' + MAP_DRV_USER + ' ' +
                                   MAP_DRV_PASS + ' /persistent:yes')
                else:
                    net_use_cmd = ('net use ' + local + r' \\' + fqdn +
                                   r'\geostorage /u:' + MAP_DRV_USER + ' ' +
                                   MAP_DRV_PASS + ' /persistent:yes')
                logger.debug('net_use_cmd: %s', net_use_cmd)
                subprocess.call(net_use_cmd, shell=True)

        # Get status again
        if not all_ok():
            logger.error('Error mapping network drives! Exiting.')
            exit(1)


def get_delay(min_, max_):
    return float('%.2f' % random.uniform(min_, max_))


def verify_worker(worker_id):

    # Delay start
    delay = get_delay((worker_id - 1) * 10 + 1, worker_id * 10)
    logger.info('[Worker-%s] Delay start for %ssecs...', worker_id, delay)
    time.sleep(delay)

    # Get directory to verify from db
    while True:
        try:
            connect_db()
            if (worker_id % 2) == 0:
                # If worker id is even, get random job
                job = (Job
                       .select()
                       .where(((Job.status == None) |
                               ((Job.status == 0) &
                                (Job.work_expiry < datetime.now()))) &
                              (Job.is_dir == True))
                       .order_by(peewee.fn.Rand())
                       .get())
            else:
                # If odd, order by dir path
                job = (Job
                       .select()
                       .where(((Job.status == None) |
                               ((Job.status == 0) &
                                (Job.work_expiry < datetime.now()))) &
                              (Job.is_dir == True))
                       .order_by(Job.dir_path)
                       .get())
            logger.info('[Worker-%s] Found job: %s:%s', worker_id,
                        job.file_server, job.dir_path)
            verify_dir(worker_id, job)
        except Exception:
            logger.exception('[Worker-%s] Error running job!', worker_id)
        finally:
            close_db()
        # Sleep
        delay = get_delay(1, 10)
        logger.info('[Worker-%s] Sleeping for %ssecs...', worker_id, delay)
        time.sleep(delay)


def verify_dir(worker_id, job):

    try:
        # Set working status
        with MYSQL_DB.atomic() as txn:
            job.status = 0
            job.work_expiry = datetime.now() + timedelta(hours=1)  # set time limit to 1hr
            job.save()

        # Get local dir path
        dir_path = os.path.abspath(os.path.join(
            FILE_SERVERS[job.file_server]['local'], job.dir_path))
        logger.info('[Worker-%s] Local directory: %s', worker_id, dir_path)
        if not os.path.isdir(dir_path):
            logger.error("[Worker-%s] %s doesn't exist! Exiting.", worker_id,
                         dir_path)
            # Invalidate directory
            with MYSQL_DB.atomic() as txn:
                job.is_dir = False
                job.save()
            return

        # Get file list
        logger.info('[Worker-%s] Getting file list...', worker_id)
        file_list = {}
        for f in sorted(os.listdir(dir_path)):
            # Ignore some files/dirs
            if ignore_file_dir(f):
                continue
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
                    fp = tail[1:].replace(os.sep, os.altsep)
                    logger.debug('[Worker-%s] fp: %s', worker_id, fp)
                    # Separate dir_path and filename
                    dir_path, filename = os.path.split(fp)
                    res['dir_path'] = dir_path
                    res['filename'] = filename
                    # Add result to db
                    db_res, created = Result.get_or_create(file_server=job.file_server,
                                                           file_path=fp,
                                                           defaults=res)
                    logger.debug('%s, %s', db_res, created)
                    # If not created, update result in db
                    if not created:
                        for k, v in res.viewitems():
                            exec('db_res.' + k + ' = v')
                        db_res.save()

            # Set done status
            job.status = 1
            job.work_expiry = None
            job.save()
            logger.info('[Worker-%s] %s:%s Done!', worker_id, job.file_server,
                        job.dir_path)

    except Exception:
        logger.exception('[Worker-%s] Error running worker!', worker_id)
        raise


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
    checksum, last_modified = get_checksums(file_path)

    file_type = None
    is_processed = True
    has_error = None
    remarks = ''
    if file_ext in RASTERS:
        file_type = 'RASTER'
        has_error, remarks = verify_raster(file_path, checksum)
    elif file_ext in VECTORS:
        file_type = 'VECTOR'
        has_error, remarks = verify_vector(file_path, checksum)
    elif file_ext in LAS:
        file_type = 'LAS/LAZ'
        has_error, remarks = verify_las(file_path, checksum)
    elif file_ext in ARCHIVES:
        file_type = 'ARCHIVE'
        has_error, remarks = verify_archive(file_path, checksum)
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
        'processor': processor,
        'ftp_suggest': None,
        'is_file': True
    }

    return file_path, result


def get_checksums(file_path, skip_checksum=False):

    dir_path, filename = os.path.split(file_path)

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
                if fn == filename:
                    checksum = tokens[0]
    if not checksum and not skip_checksum:
        # Compute checksum
        shasum = subprocess.check_output(['sha1sum', file_path])
        tokens = shasum.strip().split()
        checksum = tokens[0][1:]

    # Check if LAST_MODIFIED file already exists
    last_modified = None
    last_modified_filepath = os.path.join(dir_path, 'LAST_MODIFIED')
    if os.path.isfile(last_modified_filepath):

        last_modified_all = json.load(open(last_modified_filepath, 'r'))
        if filename in last_modified_all:
            last_modified = last_modified_all[filename]
    if not last_modified:
        # Get last modified time
        last_modified = os.stat(file_path).st_mtime

    return checksum, int(last_modified)


def verify_raster(file_path, checksum):

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

    if (output is None or
            (output and 'checksum' in output and
                output['checksum'] != checksum)):
        # Process file and redirect output to json file
        proc = subprocess.Popen(
            ['gdalinfo', '-checksum', file_path], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        out, err = proc.communicate()
        returncode = proc.returncode
        output = {'out': str(out).lower(),
                  'returncode': returncode,
                  'checksum': checksum}
        if "can't load requested dll" in output['out']:
            logger.error('Error loading requested dll! Exiting.')
            logger.error('out:\n%s', pformat(out))
            exit(1)
        json.dump(output, open(outfile, 'w'), indent=4,
                  sort_keys=True, ensure_ascii=False)

    # Save checksum in output if missing
    if 'checksum' not in output:
        output['checksum'] = checksum
        json.dump(output, open(outfile, 'w'), indent=4, sort_keys=True,
                  ensure_ascii=False)

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


def verify_vector(file_path, checksum):

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

    if (output is None or
            (output and 'checksum' in output and
                output['checksum'] != checksum)):
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

    # Save checksum in output if missing
    if 'checksum' not in output:
        output['checksum'] = checksum
        json.dump(output, open(outfile, 'w'), indent=4, sort_keys=True,
                  ensure_ascii=False)

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


def verify_las(file_path, checksum):

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

    if (output is None or
            (output and 'checksum' in output and
                output['checksum'] != checksum)):
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

    # Save checksum in output if missing
    if 'checksum' not in output:
        output['checksum'] = checksum
        json.dump(output, open(outfile, 'w'), indent=4, sort_keys=True,
                  ensure_ascii=False)

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


def verify_archive(file_path, checksum):

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

    if (output is None or
            (output and 'checksum' in output and
                output['checksum'] != checksum)):
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

    # Save checksum in output if missing
    if 'checksum' not in output:
        output['checksum'] = checksum
        json.dump(output, open(outfile, 'w'), indent=4, sort_keys=True,
                  ensure_ascii=False)

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


def upload_results():

    for dp_prefix, spreadsheetId in sorted(SHEETS.viewitems()):
        if dp_prefix == 'Summary':
            update_summary(spreadsheetId)
        elif dp_prefix == 'DPC/TERRA/LAS_Tiles':
            update_las_tiles_sheet(dp_prefix, spreadsheetId)
        else:
            update_sheet(dp_prefix, spreadsheetId)


def update_sheet(dp_prefix, spreadsheetId):

    logger.info('Updating %s sheet...', dp_prefix)

    # Create GoogleSheet object
    gs = GoogleSheet(spreadsheetId)

    # Get current values table
    sheetName = 'Sheet1'
    rangeName = sheetName + '!A:H'
    logger.info('Getting current values from Google Sheet...')
    old_values = gs.get_values(rangeName)

    # Convert values table to dict
    logger.info('Converting values to dict...')
    values_dict = {}
    has_changes = False
    has_empty_rows = False
    # Ignore header and footer
    for row in old_values[1:-1]:

        try:

            file_server = row[0]  # A
            file_path = row[1]  # B
            file_ext = row[2]  # C
            file_type = row[3]  # D
            remarks = row[4]  # E
            last_modified = row[5]  # F
            uploaded = row[6]  # G

            values_dict[(file_server, file_path)] = {
                'file_ext': file_ext,
                'file_type': file_type,
                'remarks': remarks,
                'last_modified': last_modified,
                'uploaded': uploaded,
                'valid': False
            }

            try:
                ftp_suggest = row[7]  # H
                values_dict[(file_server, file_path)][
                    'ftp_suggest'] = ftp_suggest
            except IndexError:
                # Ignore
                pass

        except IndexError:
            has_changes = True
            has_empty_rows = True

    # Get all results
    logger.info('Getting all results from db and updating dict...')
    if 'ftp' in dp_prefix:
        q = Result.select().where((Result.file_server == dp_prefix) &
                                  (Result.has_error == True) &
                                  (Result.is_file == True))
    else:
        q = Result.select().where((Result.file_path.startswith(dp_prefix)) &
                                  (Result.has_error == True) &
                                  (Result.is_file == True))
    connect_db()
    for r in q:

        k = (r.file_server, r.file_path)

        if r.uploaded is None:
            r.uploaded = datetime.now()
            with MYSQL_DB.atomic() as txn:
                r.save()

        if k not in values_dict:

            # Limit remarks to 1000 chars
            remarks = r.remarks
            if len(remarks) >= 1000:
                remarks = remarks[:997] + '...'

            last_modified = None
            try:
                last_modified = (datetime
                                 .fromtimestamp(r.last_modified)
                                 .strftime('%Y-%m-%d %H:%M:%S'))
            except ValueError:
                pass

            values_dict[k] = {
                'file_ext': r.file_ext,
                'file_type': r.file_type,
                'remarks': remarks,
                'last_modified': last_modified,
                'uploaded': r.uploaded.strftime('%b %d, %Y'),
                'ftp_suggest': r.ftp_suggest,
                'valid': True
            }

            has_changes = True

        else:
            values_dict[k]['valid'] = True

            if 'ftp_suggest' not in values_dict[k]:
                values_dict[k]['ftp_suggest'] = r.ftp_suggest
                has_changes = True

    close_db()

    # Create new values list
    logger.info('Creating new values list...')
    headers = ['file_server',
               'file_path',
               'file_ext',
               'file_type',
               'remarks',
               'last_modified',
               'uploaded',
               'ftp_suggest']
    new_values = []
    for k, v in sorted(values_dict.viewitems()):

        file_server, file_path = k

        if v['valid']:
            row = []
            row.append(file_server)
            row.append(file_path)
            row.append(v['file_ext'])
            row.append(v['file_type'])
            row.append(v['remarks'])
            row.append(v['last_modified'])
            row.append(v['uploaded'])
            row.append(v['ftp_suggest'])
        else:
            row = ['' for _ in range(len(headers))]
            has_changes = True
            has_empty_rows = True

        new_values.append(row)

    # Number of rows must match the old values to empty them from the sheet
    len_old = len(old_values) - 2  # remove header and footer
    len_new = len(new_values)
    logger.debug('len_old: %s', len_old)
    logger.debug('A: len_new: %s', len_new)
    if len_old > len_new:
        for _ in range(len_old - len_new):
            new_values.append(['' for _ in range(len(headers))])
    logger.debug('B: len_new: %s', len(new_values))

    new_values.sort()

    # Add headers
    new_values.insert(0, headers)

    # Add footer
    new_values.append(['---' for _ in range(len(headers))])

    if not has_changes:
        logger.info('No changes! Exiting.')
        return

    # Get start index of empty rows
    startIndex = None
    endIndex = None
    if has_empty_rows:
        for i in range(len(new_values)):
            if new_values[i][0] == '' and startIndex is None:
                startIndex = i
            elif new_values[i][0] != '' and startIndex and endIndex is None:
                endIndex = i
                break

        logger.debug('startIndex: %s', startIndex)
        logger.debug('endIndex: %s', endIndex)

    # Update Google Sheeet
    logger.info('Updating Google Sheet values...')
    gs.update_values(rangeName, new_values)

    logger.info('Updating Google Sheet properties...')
    if has_empty_rows:
        update_properties(gs, dp_prefix, {'sheetName': sheetName,
                                          'startIndex': startIndex,
                                          'endIndex': endIndex})
    else:
        update_properties(gs, dp_prefix)

    logger.info('Done!')


def update_summary(spreadsheetId):

    # Get summary values from db
    logger.info('Getting summary values from db...')
    values = []
    headers = ['',
               'Processed dirs',
               'Total dirs',
               'Percentage done',
               'No. of files with error',
               'Total no. of files',
               'Percentage of files with error by file count',
               'Total size of files with error (TB)',
               'Total file size (TB)',
               'Percentage of files with error by file size']
    values.append(headers)

    connect_db()
    for dp_prefix in sorted(SHEETS.viewkeys()):

        if dp_prefix == 'Summary' or dp_prefix == 'DPC/TERRA/LAS_Tiles':
            # Skip
            continue

        # Add directory prefix
        row = []
        logger.info('Processing %s...', dp_prefix)
        row.append(dp_prefix)

        # Add processed status
        if 'ftp' in dp_prefix:
            proc_dirs = (Job
                         .select()
                         .where((Job.file_server == dp_prefix) &
                                (Job.status == True) &
                                (Job.is_dir == True))
                         .count())
        else:
            proc_dirs = (Job
                         .select()
                         .where((Job.dir_path.startswith(dp_prefix)) &
                                (Job.status == True) &
                                (Job.is_dir == True))
                         .count())
        logger.debug('proc_dirs: %s', proc_dirs)
        row.append(proc_dirs)

        if 'ftp' in dp_prefix:
            totl_dirs = (Job
                         .select()
                         .where((Job.file_server == dp_prefix) &
                                (Job.is_dir == True))
                         .count())
        else:
            totl_dirs = (Job
                         .select()
                         .where((Job.dir_path.startswith(dp_prefix)) &
                                (Job.is_dir == True))
                         .count())
        logger.debug('totl_dirs: %s', totl_dirs)
        row.append(totl_dirs)

        pct_dirs = '%.2f' % (proc_dirs / totl_dirs * 100)
        logger.debug('pct_dirs: %s', pct_dirs)
        row.append(pct_dirs)

        # Add error by file count
        if 'ftp' in dp_prefix:
            err_files = (Result
                         .select()
                         .where((Result.file_server == dp_prefix) &
                                (Result.has_error == True) &
                                (Result.is_file == True))
                         .count())
        else:
            err_files = (Result
                         .select()
                         .where((Result.file_path.startswith(dp_prefix)) &
                                (Result.has_error == True) &
                                (Result.is_file == True))
                         .count())
        logger.debug('err_files: %s', err_files)
        row.append(err_files)

        if 'ftp' in dp_prefix:
            totl_files = (Result
                          .select()
                          .where((Result.file_server == dp_prefix) &
                                 (Result.is_file == True))
                          .count())
        else:
            totl_files = (Result
                          .select()
                          .where((Result.file_path.startswith(dp_prefix)) &
                                 (Result.is_file == True))
                          .count())
        logger.debug('totl_files: %s', totl_files)
        row.append(totl_files)

        pct_files = '%.2f' % (err_files / totl_files * 100)
        logger.debug('pct_files: %s', pct_files)
        row.append(pct_files)

        # Add error by file size
        if 'ftp' in dp_prefix:
            err_size = (Result
                        .select(peewee.fn.SUM(Result.file_size))
                        .where((Result.file_server == dp_prefix) &
                               (Result.has_error == True) &
                               (Result.is_file == True))
                        .scalar())
        else:
            err_size = (Result
                        .select(peewee.fn.SUM(Result.file_size))
                        .where((Result.file_path.startswith(dp_prefix)) &
                               (Result.has_error == True) &
                               (Result.is_file == True))
                        .scalar())
        logger.debug('err_size: %s', err_size)
        row.append('%.2f' % (err_size / (1024 ** 4)))

        if 'ftp' in dp_prefix:
            totl_size = (Result
                         .select(peewee.fn.SUM(Result.file_size))
                         .where((Result.file_server == dp_prefix) &
                                (Result.is_file == True))
                         .scalar())
        else:
            totl_size = (Result
                         .select(peewee.fn.SUM(Result.file_size))
                         .where((Result.file_path.startswith(dp_prefix)) &
                                (Result.is_file == True))
                         .scalar())
        logger.debug('totl_size: %s', totl_size)
        row.append('%.2f' % (totl_size / (1024 ** 4)))

        pct_size = '%.2f' % (err_size / totl_size * 100)
        logger.debug('pct_size: %s', pct_size)
        row.append(pct_size)

        values.append(row)
    close_db()

    values.append(['' for _ in range(10)])
    values.append(['as of ' + (datetime
                               .now()
                               .strftime('%b %d, %Y %H:%M:%S'))] +
                  ['' for _ in range(9)])

    # Update Google Sheeet
    logger.info('Updating Google Sheet...')
    gs = GoogleSheet(spreadsheetId)
    rangeName = 'Sheet2!A:J'
    gs.update_values(rangeName, values)

    logger.info('Done!')


def update_las_tiles_sheet(dp_prefix, spreadsheetId, has_error_only=True):

    logger.info('Updating %s sheet...', dp_prefix)

    # Create GoogleSheet object
    gs = GoogleSheet(spreadsheetId)

    # Get current values table
    sheetName = 'Sheet1'
    rangeName = sheetName + '!A:G'
    logger.info('Getting current values from Google Sheet...')
    old_values = gs.get_values(rangeName)

    # Convert values table to dict
    logger.info('Converting values to dict...')
    values_dict = {}
    has_changes = False
    has_empty_rows = False
    # Ignore header and footer
    for row in old_values[1:-1]:

        try:

            file_server = row[0]  # A
            block = row[1]  # B
            las_only = row[2]  # C
            laz_only = row[3]  # D
            las_n_laz = row[4]  # E
            uploaded = row[5]  # F

            values_dict[(file_server, block)] = {
                'las_only': las_only,
                'laz_only': laz_only,
                'las_n_laz': las_n_laz,
                'uploaded': uploaded,
                'valid': False
            }

            try:
                ftp_suggest = row[6]  # G
                values_dict[(file_server, block)]['ftp_suggest'] = ftp_suggest
            except IndexError:
                # Ignore
                pass

        except IndexError:
            has_changes = True
            has_empty_rows = True

    # Get all results
    logger.info('Getting all results from db and updating dict...')
    q = Result.select().where((Result.has_error == True) &
                              (Result.file_path.contains(dp_prefix +
                                                         '%LAS_FILES')) &
                              (Result.file_type == 'LAS/LAZ') &
                              (Result.is_file == True))
    # Collate results by block
    cur_block = ''
    las_set = set()
    laz_set = set()
    uploaded = datetime.min
    ftp_suggest_set = set()
    connect_db()
    for r in q:

        # Get block name
        path_tokens = r.file_path.split(os.sep)
        block = path_tokens[4]

        # Initialize current block
        if cur_block != block:

            # Add previous block to values dict
            if cur_block and (las_set or laz_set):
                k = r.file_server, cur_block
                las_only = ','.join([str(x) for x in sorted(las_set)])
                laz_only = ','.join([str(x) for x in sorted(laz_set)])
                las_n_laz = ','.join([str(x)
                                      for x in sorted(las_set & laz_set)])
                ftp_suggest = '\n'.join(sorted(ftp_suggest_set))

                # Check if either the block or the las/laz file list is new
                if (k not in values_dict or
                    (k in values_dict and
                        las_only != values_dict[k]['las_only'] or
                        laz_only != values_dict[k]['laz_only'] or
                        las_n_laz != values_dict[k]['las_n_laz'] or
                        'ftp_suggest' not in values_dict[k] or
                        ('ftp_suggest' in values_dict[k] and
                         ftp_suggest != values_dict[k]['ftp_suggest']))):

                    values_dict[k] = {
                        'las_only': las_only,
                        'laz_only': laz_only,
                        'las_n_laz': las_n_laz,
                        'uploaded': uploaded.strftime('%b %d, %Y'),
                        'ftp_suggest': ftp_suggest,
                        'valid': True
                    }

                    has_changes = True

                else:
                    values_dict[k]['valid'] = True

            # Reset current block
            cur_block = block
            las_set = set()
            laz_set = set()
            uploaded = datetime.min
            ftp_suggest_set = set()

        # Get file name and ext
        fn, ext = os.path.splitext(path_tokens[-1])

        # Get pt no.
        try:
            # Try getting pt no
            pt_no = int(fn[2:])
        except Exception:
            # If not, just get the filename
            pt_no = fn

        # Add pt no. to set
        if ext == '.las':
            las_set.add(pt_no)
        elif ext == '.laz':
            laz_set.add(pt_no)

        # Get latest uploaded time
        if r.uploaded and r.uploaded > uploaded:
            uploaded = r.uploaded
        elif r.uploaded is None:
            uploaded = datetime.now()

        # Get ftp suggestions
        if r.ftp_suggest:
            for p in r.ftp_suggest.split('\n'):
                dp = os.path.dirname(p)
                ftp_suggest_set.add(dp)

    close_db()

    # Create new values list
    logger.info('Creating new values list...')
    headers = ['file_server',
               'block',
               'las_only',
               'laz_only',
               'las_n_laz',
               'uploaded',
               'ftp_suggest']
    new_values = []
    for k, v in sorted(values_dict.viewitems()):

        file_server, block = k

        if v['valid']:
            row = []
            row.append(file_server)
            row.append(block)
            row.append(v['las_only'])
            row.append(v['laz_only'])
            row.append(v['las_n_laz'])
            row.append(v['uploaded'])
            row.append(v['ftp_suggest'])
        else:
            row = ['' for _ in range(len(headers))]
            has_changes = True
            has_empty_rows = True

        new_values.append(row)

    # Number of rows must match the old values to empty them from the sheet
    len_old = len(old_values) - 2  # remove header and footer
    len_new = len(new_values)
    logger.debug('len_old: %s', len_old)
    logger.debug('A: len_new: %s', len_new)
    if len_old > len_new:
        for _ in range(len_old - len_new):
            new_values.append(['' for _ in range(len(headers))])
    logger.debug('B: len_new: %s', len(new_values))

    new_values.sort()

    # Add headers
    new_values.insert(0, headers)

    # Add footer
    new_values.append(['---' for _ in range(len(headers))])

    if not has_changes:
        logger.info('No changes! Exiting.')
        return

    # Get start index of empty rows
    startIndex = None
    endIndex = None
    if has_empty_rows:
        for i in range(len(new_values)):
            if new_values[i][0] == '' and startIndex is None:
                startIndex = i
            elif new_values[i][0] != '' and startIndex and endIndex is None:
                endIndex = i
                break

        logger.debug('startIndex: %s', startIndex)
        logger.debug('endIndex: %s', endIndex)

    # Update Google Sheeet
    logger.info('Updating Google Sheet...')
    gs.update_values(rangeName, new_values)

    logger.info('Updating Google Sheet properties...')
    if has_empty_rows:
        update_properties(gs, dp_prefix, {'sheetName': sheetName,
                                          'startIndex': startIndex,
                                          'endIndex': endIndex})
    else:
        update_properties(gs, dp_prefix)

    logger.info('Done!')


def update_properties(gs, dp_prefix, delete_rows=None):
    logger.debug('delete_rows: %s', pformat(delete_rows, width=40))
    title = (dp_prefix + ' corrupted list (' +
             datetime.now().strftime('%b %d, %Y') + ')')
    requests = []
    requests.append({
        'updateSpreadsheetProperties': {
            'properties': {
                'title': title
            },
            'fields': 'title'
        }
    })
    if delete_rows:
        sheetId = gs.get_sheet_id(delete_rows['sheetName'])
        logger.debug('sheetId: %s', sheetId)
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': sheetId,
                    'dimension': 'ROWS',
                    'startIndex': delete_rows['startIndex'],
                    'endIndex': delete_rows['endIndex']
                }
            }
        })
    gs.batch_update(requests)


def find_ftp_suggestions():
    try:
        connect_db()
        pool = multiprocessing.Pool(processes=WORKERS)
        for k, token_id in sorted(BLOCK_NAME_INDEX.viewitems()):
            logger.info('Finding FTP suggestions for %s...', k)
            q = Result.select().where((Result.file_path.startswith(k)) &
                                      (Result.has_error == True) &
                                      (Result.is_file == True))
            counter = 0
            for r in q:
                pool.apply_async(find_ftp_suggestions_worker, (r, token_id))
                counter += 1
            logger.info('counter: %s', counter)

        pool.close()
        pool.join()
    except Exception:
        logger.exception('Error finding ftp suggestions!')
    finally:
        close_db()


def find_ftp_suggestions_worker(result, token_id):
    try:
        connect_db()

        # Get process id
        pid = multiprocessing.current_process().pid

        # Get block name
        logger.debug('[Worker-%s] result.file_path: %s', pid, result.file_path)
        if os.name == 'nt':
            path_tokens = result.file_path.split(os.altsep)
        else:
            path_tokens = result.file_path.split(os.sep)
        if len(path_tokens) >= token_id + 1:
            block_name = path_tokens[token_id]
            logger.debug('[Worker-%s] block_name: %s', pid, block_name)

            # Get filename
            filename = os.path.basename(result.file_path)
            logger.debug('[Worker-%s] filename: %s', pid, filename)

            # Find file from ftp
            ftp_suggest_buf = []
            match_str = '%' + block_name + '%' + filename
            logger.debug('[Worker-%s] match_str: %s', pid, match_str)
            exact_match = None
            try:
                # Try exact match 1st
                exact_match = (Result
                               .select()
                               .where((Result.has_error == False) &
                                      (Result.is_file == True) &
                                      (Result.file_server == "ftp01") &
                                      (Result.file_path % match_str))
                               .get())
            except Result.DoesNotExist:
                pass

            if exact_match:
                logger.debug('[Worker-%s] Exact: %s',
                             pid, exact_match.file_path)
                # break
                ftp_suggest_buf.append(
                    'ftp01:' + exact_match.file_path)
            else:
                # If no exact match, try similar search
                similar_query = Result.raw("""
SELECT *
FROM result
WHERE has_error = %s AND
is_file = %s AND
file_server = %s AND
filename = %s
ORDER BY jaro_winkler_similarity(dir_path, %s) DESC
LIMIT 10""",
                                           False, True, 'ftp01',
                                           filename, block_name)
                for similar_match in similar_query.execute():
                    logger.debug('[Worker-%s] Similar: %s',
                                 pid, similar_match.file_path)
                    ftp_suggest_buf.append(
                        'ftp01:' + similar_match.file_path)

            result.ftp_suggest = '\n'.join(ftp_suggest_buf)
            # Save
            with MYSQL_DB.atomic() as txn:
                result.save()

    except Exception:
        logger.exception('Error running find ftp suggestions worker! (%s)',
                         result.file_path)
    finally:
        close_db()

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
        update_dir(args.update_dir_path)

    elif 'start_target' in args:

        if args.start_target == 'workers':

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
                    threading.Thread(target=verify_worker,
                                     args=(worker_id,)).start()
                except Exception:
                    logger.exception('[Worker-%s] Error running worker!',
                                     worker_id)

        elif args.start_target == 'suggest':
            find_ftp_suggestions()

        elif args.start_target == 'migrate':
            migrate04()

    elif 'upload_target' in args:

        if args.upload_target == 'results':

            logger.info('Uploading results...')
            upload_results()

    logger.info('All done!')
