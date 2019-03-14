import argparse
from multiprocessing.pool import ThreadPool
import numpy as np
import os
import time


def GetChunk(filename, start, end):
    with open(filename, 'rb') as fin:
        fin.seek(start)
        data[start:end] = np.fromfile(fin, dtype=np.int8, count=end-start)
    print(start, end)
    return len(data)


def MPFileReader(filename, processes, chunksize, cap=None):
    # Threadpool to do the work
    pool = ThreadPool(processes)

    # Size of the dataset to read
    datasize = os.path.getsize(filename)
    if cap is not None:
        datasize = cap

    # Allocate our datasets
    dataloc = np.empty((datasize,), dtype=np.int8)

    # Location offsets to use
    starts = list(range(0, datasize, chunksize))
    ends = starts[1:] + [datasize]

    arguments = zip([filename]*len(starts), starts, ends)

    for i, l in enumerate(pool.starmap(GetChunk, arguments)):
        pass

    return dataloc


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-d', '--datafile', required=True, help='File to test')
    PARSER.add_argument('-p', '--processes', default=8, type=int, help='Number of processes to use')
    PARSER.add_argument('-c', '--chunksize', default=1000000, type=int, help='Size of chunks to read')
    PARSER.add_argument('-a', '--cap', type=int, default=None, help='Cap the filesize (test)')
    ARGS = PARSER.parse_args()

    start = time.time()
    dloc = MPFileReader(ARGS.datafile, ARGS.processes, ARGS.chunksize, ARGS.cap)
    print(time.time() - start)

    print(dloc[100:1000].tobytes())