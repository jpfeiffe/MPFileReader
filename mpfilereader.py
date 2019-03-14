import argparse
import multiprocessing as mp
import numpy as np
import os
import time


def MPFileReader(filename, processes, chunksize):
    def GetChunk(args):
        filename = args[0]
        cs = args[1]
        offset = args[2]
        with open(file, 'rb') as fin:
            fin.seek(offset)
            data = fin.read(cs)
        return offset

    offsets = list(range(0, datasize, cs))
    offsets = list(zip([filename]*len(offsets), [chunksize]*len(offsets), offsets))
    for offset, data in pool.map(f, offsets):
        print(len(data))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-d', '--datafile', required=True, help='File to test')
    PARSER.add_argument('-p', '--processes', default=8, type=int, help='Number of processes to use')
    PARSER.add_argument('-c', '--chunksize', default=1000000, type=int, help='Size of chunks to read')
    ARGS = PARSER.parse_args()

    MPFileReader(ARGS.datafile, ARGS.processes, ARGS.chunksize)