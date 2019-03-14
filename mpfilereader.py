import argparse
from multiprocessing.pool import ThreadPool
import numpy as np
import os
import time
import logging


def GetChunk(dataloc, filename, start, end):
    logging.debug(f'Beginning {start} to {end}')
    with open(filename, 'rb') as fin:
        fin.seek(start)
        dataloc[start:end] = np.fromfile(fin, dtype=np.int8, count=end-start)
    logging.debug(f'Endings {start} to {end}')
    return start,end


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

    arguments = zip([dataloc]*len(starts), [filename]*len(starts), starts, ends)

    for i, (start,end) in enumerate(pool.starmap(GetChunk, arguments)):
        logging.debug(f'Finished {start} to {end}')

    return dataloc


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-d', '--datafile', required=True, help='File to test')
    PARSER.add_argument('-p', '--processes', default=8, type=int, help='Number of processes to use')
    PARSER.add_argument('-c', '--chunksize', default=1000000, type=int, help='Size of chunks to read')
    PARSER.add_argument('-a', '--cap', type=int, default=None, help='Cap the filesize (test)')
    PARSER.add_argument('-v', '--validate', action='store_true', help='validatet the files are identical (only use on small data; will reload file!)')
    PARSER.set_defaults(validate=False)
    ARGS = PARSER.parse_args()
    logging.basicConfig(level=logging.DEBUG)

    start = time.time()
    dloc = MPFileReader(ARGS.datafile, ARGS.processes, ARGS.chunksize, ARGS.cap)
    logging.info(f'Time was {time.time() - start}')

    if ARGS.validate:
        logging.info(f'Starting Validation')
        dloc2 = open(ARGS.datafile, 'rb').read()
        logging.info(f'Validation File Loaded...')
        logging.info(f'Validation: {all(a == b for a, b in zip(dloc.tobytes(), dloc2))}')
    