import argparse
from multiprocessing.pool import ThreadPool
# import numpy as np
import os
import time


def GetChunk(dataloc, filename, start, end):
    print('Beginning', start, end)
    with open(filename, 'rb') as fin:
        fin.seek(start)
        print('Waiting', start, end)
        dataloc[start:end] = fin.read(end-start)
    print('Ending', start, end)
    return start,end


def MPFileReader(filename, processes, chunksize, cap=None):
    # Threadpool to do the work
    pool = ThreadPool(processes)

    # Size of the dataset to read
    datasize = os.path.getsize(filename)
    if cap is not None:
        datasize = cap

    # Allocate our datasets
    dataloc = bytes(datasize)

    # Location offsets to use
    starts = list(range(0, datasize, chunksize))
    ends = starts[1:] + [datasize]

    arguments = zip([dataloc]*len(starts), [filename]*len(starts), starts, ends)

    for i, (start,end) in enumerate(pool.starmap(GetChunk, arguments)):
        print('Finished', start,end)

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

    print(dloc[0:20])
    print(open(ARGS.datafile, 'rb').read(20))