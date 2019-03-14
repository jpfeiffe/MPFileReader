import argparse
import multithreading as mp
import numpy as np
import os
import time


def GetChunk(args):
    filename = args[0]
    cs = args[1]
    offset = args[2]
    with open(filename, 'rb') as fin:
        fin.seek(offset)
        data = fin.read(cs)
    print(len(data), offset)
    return data, offset, len(data)


def MPFileReader(filename, processes, chunksize, cap=None):
    pool = mp.Pool(processes)
    datasize = os.path.getsize(filename)
    
    dataloc = np.empty((datasize, 1), dtype=np.int8)

    print(datasize, cap)

    offsets = list(range(0, min(datasize, cap), chunksize))
    print(offsets)

    offsets = list(zip([filename]*len(offsets), [chunksize]*len(offsets), offsets))

    print(offsets)
    for data, offset, datalen in pool.map(GetChunk, offsets):
        print()
        dataloc[offset:offset+datalen] = data


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-d', '--datafile', required=True, help='File to test')
    PARSER.add_argument('-p', '--processes', default=8, type=int, help='Number of processes to use')
    PARSER.add_argument('-c', '--chunksize', default=1000000, type=int, help='Size of chunks to read')
    PARSER.add_argument('-a', '--cap', type=int, default=None, help='Cap the filesize (test)')
    ARGS = PARSER.parse_args()

    start = time.time()
    MPFileReader(ARGS.datafile, ARGS.processes, ARGS.chunksize, ARGS.cap)
    print(time.time() - start)