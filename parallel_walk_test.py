#!/usr/bin/env python

from settings import *
import multiprocessing
import os
import sys


def worker(dir_path, dir_paths):

    if os.path.isdir(dir_path):

        for i in sorted(os.listdir(dir_path)):
            # Ignore hidden files/dirs
            if i.startswith('.'):
                continue

            i_path = os.path.join(dir_path, i)
            if os.path.isdir(i_path):
                dir_paths.put(i_path)
            elif os.path.isfile(i_path):
                print i_path

    dir_paths.put('no-dir')


if __name__ == '__main__':
    manager = multiprocessing.Manager()
    pool = multiprocessing.Pool(processes=WORKERS)
    dir_paths = manager.Queue()

    rootdir_path = os.path.abspath(sys.argv[1])

    counter = 1
    pool.apply_async(worker, (rootdir_path, dir_paths))

    while counter > 0:
        dir_path = dir_paths.get()
        if dir_path == 'no-dir':
            # dir has finished processing
            counter -= 1
        else:
            # a new dir needs to be processed
            counter += 1
            pool.apply_async(worker, (dir_path, dir_paths))

    pool.close()
    pool.join()
