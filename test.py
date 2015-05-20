"""
    Multiprocessing in python
    Copyright (C) 2015 Jordi Pujol-Ahullo <jordi.pujol@urv.cat>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
import sys
import re
import py_performance
import line_profiler
from memory_profiler import profile
from memory_profiler import memory_usage
import pdb
import py_ecc
import random
import multiprocessing
from multiprocessing import Process, Manager, Array, Value, Lock

lock = Lock() # Global definition of lock


import logging
import math

import time
from functools import wraps
from guppy import hpy



g_totalTimeElapsed = 0
g_minTime = sys.float_info.max
g_maxTime = 0

#Decorator function for time profiling
def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        # print ("Total time running %s: %s seconds" %
        #        (function.func_name, str(t1 - t0))
        # );
        timeElapsed = t1 - t0
        global g_totalTimeElapsed
        global g_minTime
        global g_maxTime
        g_totalTimeElapsed = g_totalTimeElapsed + timeElapsed
        if timeElapsed > g_maxTime:
            g_maxTime = timeElapsed
        if timeElapsed < g_minTime:
            g_minTime = timeElapsed
        return result

    return function_timer


def _run_tests():
    """
    This runs all unitary tests from the py_ecc package.

    In particular:
        py_ecc.rs_code._test()
        py_ecc.file_ecc._test()
    """
    print "Running Reed Salomon self tests...",
    print py_ecc.rs_code._test(), ". Done!"
    print "Running self tests for error correction on files...",
    print py_ecc.file_ecc._test(), ". Done!"


def worker(data):
    """
    This worker simply returns the square value.
    """
    return data * data


def test_pool(size):
    """
    This test runs a pool of processes and calculates the squares of a list of
    integer values.
    """
    print "### Running test_pool with {} processes".format(size)
    p = multiprocessing.Pool(size)
    data = range(3)

    print data, "=>", p.map(worker, data)
    p.terminate()
    p.join()
    print ""


def worker2(data):
    """
    This worker calculates the square of the integers from the given list.
    """
    work = []
    for v in data:
        work.append(v * v)
    return work


def test_pool2(size):
    """
    This test runs a pool of processes and calculates the square values
    from a list of list of integers.
    """
    print "### Running test_pool2 with {} processes".format(size)
    p = multiprocessing.Pool(size)
    data = [range(2 * i) for i in range(2 * size)]

    returnedData = p.map(worker2, data)
    for i in range(len(data)):
        print data[i]
        print returnedData[i]
        print ""

    p.terminate()
    p.join()


def fail_workers(pool, failures):
    """
    This function emulates failing nodes/processes by terminating the
    number of "failures" processes from the "pool".
    """
    if failures > pool._processes:
        raise Exception(
            "You want to fail {} workers from a total of {}, but you can't!!".format(failures, pool._processes))

    ids = random.sample(range(pool._processes), failures)
    for i in ids:
        "emulating a worker fails via its terminate()"
        pool._pool[i].terminate()
        pool._pool[i].join()

    "after failing processes, we need to recover the amount of processes in the pool"
    pool._maintain_pool()


def test_pool_failing_workers(size, failures):
    """
    This test emulates failing "failures" workers from a pool of "size" number of workers.
    """
    print "### Running pool test and emulate workers stop randomly"
    # enable_debug()
    p = multiprocessing.Pool(size)
    print "Workers => ", p._pool
    print "Workers to make fail:", failures
    fail_workers(p, failures)
    print "Workers after failures:", p._pool
    print ""
    p.terminate()
    p.join()


def who_i_am(data):
    """
    The job of this worker is simply tell who it is ;-)
    """
    print "Hi! I'm {} and I'm processing {}!".format(multiprocessing.current_process().name, data)


# this is random
def test_pool_who_i_am(size):
    """
    This test shows the way of knowing which process is dealing with
    each piece of data.
    We discover that the load is not uniformly distributed among processes, but data-ordered.
    """
    print "### Running pool test for process introspection"
    p = multiprocessing.Pool(size)
    data = range(size * 2)
    datalist = [[i, i + 1] for i in range(2 * size)]
    "this time, we don't expect any result from the workers."
    p.map(who_i_am, data)
    p.map(who_i_am, datalist)
    print ""
    p.terminate()
    p.join()


# this is uniform
def test_pool_who_i_am_uniform(size):
    """
    This test forces a uniform distribution of workload among processes.
    To do so, we implement a pool of Pools for simplicity.
    """
    print "### Running pool test for uniform distribution of workload"
    p = [multiprocessing.Pool(1) for i in range(size)]

    data = range(size * 2)
    datalist = [[i] for i in range(2 * size)]
    datalist2 = [[i, i + 1] for i in range(2 * size)]
    "this time, we don't expect any result from the workers."
    "p.map(who_i_am, data)"
    for i, datum in enumerate(data):
        p[i % size].apply(who_i_am, (datum,))

    "p.map(who_i_am, datalist)"
    for i, datum in enumerate(datalist):
        p[i % size].apply(who_i_am, (datum,))

    "p.map(who_i_am, datalist2)"
    for i, datum in enumerate(datalist2):
        p[i % size].apply(who_i_am, (datum,))

    for pool in p:
        pool.terminate()
        pool.join()
    print ""


def enable_debug():
    """
    Enables the full debug, including for sub processes.
    """
    logger = multiprocessing.log_to_stderr(logging.DEBUG)
    logger.setLevel(multiprocessing.SUBDEBUG)


# @profile
@fn_timer
def map_reduce(clusterSize, logData, pool, threadLoadMap, threadLoadCombine,
               threadLoadReduce, threadBandwidthIn, threadBandwidthOut, reduceCounter):
    """
    Runs non-uniform distribution version of the map reduce algorithm
    :param clusterSize: number of threads
    :param logData: data to process
    :param pool: pool of workers
    :param other: variables used for statistics
    :return: result
    """

    # Fragment the input log into @clusterSize chunks
    logLines = len(logData)
    partitionLength = clusterSize;
    logChunkSize = int(math.ceil(logLines / partitionLength))

    list = [x for x in xrange(0, len(logData) + 1, logChunkSize)]
    list[-1] = logLines  # fix the last offset
    # SPLIT
    logChunkList = lindexsplit(logData, list)

    # MAP
    map_visitor = pool.map(Map, logChunkList)
    # Save statistics into variables
    for x in map_visitor:
        try:
            threadLoadMap[x[1][0]] += x[1][1]
            threadBandwidthIn[x[1][0]] += x[2][0]
            threadBandwidthOut[x[1][0]] += x[2][1]
        except KeyError:
            threadLoadMap[x[1][0]] = x[1][1]
            # print "k:", x[2][0], "v: ", x[2][1]
            threadBandwidthIn[x[1][0]] = x[2][0]
            threadBandwidthOut[x[1][0]] = x[2][1]

    list = [(x[0]) for x in map_visitor];

    # Setup Shared Memory
    toShare = Manager()
    combined = toShare.dict()

    mapped = ((item, combined) for item in list)
    # Combine
    combineStatistics = pool.map(Combiner, mapped)

    # Save statistics into variables
    for x in combineStatistics:
        try:
            threadLoadCombine[x[0][0]] += x[0][1]
        except KeyError:
            threadLoadCombine[x[0][0]] = x[0][1]
        try:
            threadBandwidthIn[x[0][0]] += x[1][0]
            threadBandwidthOut[x[0][0]] += x[1][1]
        except KeyError:
            threadBandwidthIn[x[0][0]] = x[1][0]
            threadBandwidthOut[x[0][0]] = x[1][1]

    # Reduce
    visitor_frequency = pool.map(Reduce, combined.items())

    # Save statistics into variables
    for x in visitor_frequency:
        try:
            threadLoadReduce[x[2][0]] += x[2][1]
            reduceCounter[x[2][0]] += 1;
        except KeyError:
            threadLoadReduce[x[2][0]] = x[2][1]
            reduceCounter[x[2][0]] = 1;
        try:
            threadBandwidthIn[x[2][0]] += x[3][0]
            threadBandwidthIn[x[2][0]] += x[3][1]
        except KeyError:
            threadBandwidthIn[x[2][0]] = x[3][0]
            threadBandwidthIn[x[2][0]] = x[3][1]

    return visitor_frequency

    print "map_reduce/Finish!"

@fn_timer
def map_reduce_uniform(clusterSize, logData, pool, threadLoadMap, threadLoadCombine,
                       threadLoadReduce, threadBandwidthIn, threadBandwidthOut, reduceCounter):
    """
    Runs uniform distribution version of the map reduce algorithm
    :param clusterSize: number of threads
    :param logData: data to process
    :param pool: pool of workers
    :param other: variables used for statistics
    :return: result
    """

    # Fragment the input log into @clusterSize chunks
    logLines = len(logData)
    partitionLength = clusterSize;
    logChunkSize = int(math.ceil(logLines / partitionLength))
    list = [x for x in xrange(0, len(logData) + 1, logChunkSize)]
    list[-1] = logLines  # fix the last offset
    # SPLIT
    logChunkList = lindexsplit(logData, list)

    map_visitor = []
    # Map
    for i, data in enumerate(logChunkList):
        map_visitor.append(pool[i % clusterSize].apply(Map, [data]))

    # Save statistics into variables
    for x in map_visitor:
        try:
            threadLoadMap[x[1][0]] += x[1][1]
        except KeyError:
            threadLoadMap[x[1][0]] = x[1][1]
        try:
            threadBandwidthIn[x[1][0]] += x[2][0]
            threadBandwidthOut[x[1][0]] += x[2][1]
        except KeyError:
            threadBandwidthIn[x[1][0]] = x[2][0]
            threadBandwidthOut[x[1][0]] = x[2][1]

    # Setup Shared Memory
    toShare = Manager()
    combined = toShare.dict()

    list = []
    for x in map_visitor:
        list.append(x[0])

    precombined = ((item, combined) for item in list)
    combineStatistics = []

    # Combine
    for i, data in enumerate(precombined):
        combineStatistics.append(pool[i % clusterSize].apply(Combiner, [data]))

    # Save statistics into variables
    for x in combineStatistics:
        try:
            threadLoadCombine[x[0][0]] += x[0][1]
        except KeyError:
            threadLoadCombine[x[0][0]] = x[0][1]
        try:
            threadBandwidthIn[x[0][0]] += x[1][0]
            threadBandwidthOut[x[0][0]] += x[1][1]
        except KeyError:
            threadBandwidthIn[x[0][0]] = x[1][0]
            threadBandwidthOut[x[0][0]] = x[1][1]

    # REDUCE
    visitor_frequency = []
    for i, data in enumerate(combined.items()):
        visitor_frequency.append(pool[i % clusterSize].apply(Reduce, (data,)))

    for x in visitor_frequency:
        try:
            threadLoadReduce[x[2][0]] += x[2][1]
            reduceCounter[x[2][0]] += 1;
        except KeyError:
            threadLoadReduce[x[2][0]] = x[2][1]
            reduceCounter[x[2][0]] = 1;
        try:
            threadBandwidthIn[x[2][0]] += x[3][0]
            threadBandwidthIn[x[2][0]] += x[3][1]
        except KeyError:
            threadBandwidthIn[x[2][0]] = x[3][0]
            threadBandwidthIn[x[2][0]] = x[3][1]
    return visitor_frequency

# @fn_timer
# @profile
def Map(L):
    results = {}  # key value storage
    for line in L:
        key = str(line[0] +":" + line[1] +":" + line[2]);
        try:
            results[key] += 1
        except KeyError:
            results[key] = 1
    return results, [multiprocessing.current_process().name, memory_usage(-1, interval=.0001, timeout=.0001).pop()], [sys.getsizeof(L), sys.getsizeof(results)]

# Not used in the last version
# @fn_timer
def Partition(L):
    # print "Partition"
    tf = {}
    for sublist in L:
        for p in sublist:
            # Append the tuple to the list in the map
            try:
                tf[p].append(sublist[p])
            except KeyError:
                tf[p] = [sublist[p]]
    return tf

# @fn_timer
def Combiner(L):
    global lock
    lock.acquire()
    data = L[0]
    sizeOut = 0
    for line in data:
        sizeOut += sys.getsizeof(data[line])
        try:
            L[1][line].append(data[line])
        except KeyError:
            L[1][line] = [data[line]]
    lock.release()
    return [multiprocessing.current_process().name, memory_usage(-1, interval=.0001, timeout=.0001).pop()], [sys.getsizeof(L), sizeOut]


# @fn_timer
def Reduce(Mapping):
    sumOfMappings = sum(pair for pair in Mapping[1])
    return Mapping[0], sumOfMappings, [multiprocessing.current_process().name, memory_usage(-1, interval=.0001, timeout=.0001).pop()], \
           [sys.getsizeof(Mapping), (sys.getsizeof(Mapping[0]) + sys.getsizeof(sumOfMappings))]


@fn_timer
def load(path):
    print "load/" + path
    hp = hpy()
    # print "Heap at the beginning of the function\n", hp.heap()
    file_rows = []
    row = []
    f = open(path, "r")
    for line in f:
        row = re.split(r'\t+', line.rstrip('\t'))
        file_rows.append([row[0],row[5],row[4]])
    # add try catch handle error???
    # pdb.set_trace()
    # print "Heap at the end of the function\n", hp.heap()
    return file_rows

"""
Magic tuple sorting by ...
"""

# @fn_timer
def tuple_sort(a, b):
    if a[1] < b[1]:
        return 1
    elif a[1] > b[1]:
        return -1
    else:
        return cmp(a[1], b[1])

"""
Partition the loglist
"""
# @fn_timer
def lindexsplit(some_list, list):
    # Checks to see if any extra arguments were passed. If so,
    # prepend the 0th index and append the final index of the
    # passed list. This saves from having to check for the beginning
    # and end of args in the for-loop. Also, increment each value in
    # args to get the desired behavior.

    # For a little more brevity, here is the list comprehension of the following
    # statements:
    # return [some_list[start:end] for start, end in zip(args, args[1:])]
    my_list = []
    for start, end in zip(list, list[1:]):
        my_list.append(some_list[start:end])
    return my_list


def parseFrequency(visitor_frequency, filename):
    """
    Prints results into a file
    :param visitor_frequency: data to print into a file
    :param filename: name of the file
    :return:
    """
    visitor_frequency.sort()
    f = open(filename, 'w')
    for x in visitor_frequency:
        split = re.split('. |:', x[0])
        f.write(split[0] + " ")
        f.write(split[1] + " ")
        f.write(split[2] + " ")
        f.write(str(x[1]) + "\n")
    f.close()

def writeVarToFile(data, filename):
    # Function used for debugging
    # data.sort()
    f = open(filename, 'w')
    for x in data:
        f.write(str(x) + "\n")
    f.close()

def writeStrToFile(data, filename):
    # Function used for debugging
    # data.sort()
    f = open(filename, 'w')
    for x in data:
        f.write(str(x) + "\n")
    f.close()

if __name__ == "__main__":

    if (len(sys.argv) != 1):
        print "Program arguments...";
        print sys.argv
        sys.exit(1);

    print "main/start:"

    logFile = load("file/logs.txt")
    clusterSize = [4, 8, 16]  # nodes

    # Properly set global variables
    g_totalTimeElapsed = 0
    g_minTime = sys.float_info.max
    g_maxTime = 0
    # Test type
    uniform = 0 # 0 = Non-uniform, 1 = Uniform
    numberOfRuns = 1
    clusterSizeIndex = 0 # 0 = 4 threads, 1 = 8 threads, 2 = 16 threads
    profileMemory = 1 # 1 = profile memory, 2 = do not profile memory
    # Test statistics
    totalMemory = 0
    maxMemory = 0
    minMemory = sys.float_info.max;
    threadLoadMap = {}
    averageThreadLoadMap = 0
    threadLoadCombine = {}
    averageThreadLoadCombine = 0
    threadLoadReduce = {}
    averageThreadLoadReduce = 0
    threadBandwidthIn = {}
    threadBandwidthOut = {}
    reduceCounter = {}
    # Pool of threads
    pool = None
    # Result
    visitor_frequency = None

    # Create threads based on the type of test (uniform, non-uniform)
    if uniform == 1:
        pool = [multiprocessing.Pool(1) for i in range(clusterSize[clusterSizeIndex])]
    else:
        pool = multiprocessing.Pool(processes=clusterSize[clusterSizeIndex])
    #     Test started and will be run numberOfRuns times
    print ">>>>>>>>>>>>>>> START: cluster size: ", clusterSize[clusterSizeIndex], "Uniform: ", uniform
    for y in range (0, numberOfRuns):
        print(y),
        if uniform == 1:
            visitor_frequency = map_reduce_uniform(clusterSize[clusterSizeIndex], logFile, pool, threadLoadMap,
                                                   threadLoadCombine, threadLoadReduce, threadBandwidthIn, threadBandwidthOut, reduceCounter);
            parseFrequency(visitor_frequency, "file/out/log_uniforms.txt")
        else:
            visitor_frequency = map_reduce(clusterSize[clusterSizeIndex], logFile,pool, threadLoadMap,
                                           threadLoadCombine, threadLoadReduce, threadBandwidthIn, threadBandwidthOut, reduceCounter)
            parseFrequency(visitor_frequency, "file/out/log_non_uniforms.txt")
        #     Save memory statistics
        if profileMemory == 1:
            memoryUsed = memory_usage(-1, interval=.2, timeout=.2).pop()
            if maxMemory < memoryUsed:
                maxMemory = memoryUsed
            if minMemory > memoryUsed:
                minMemory = memoryUsed
            totalMemory += memoryUsed

    print "\n:::Speed statistics::::"
    print "Cluster size: ", clusterSize[clusterSizeIndex]
    print "Number of runs: ", numberOfRuns
    print "Total time elapsed: ", g_totalTimeElapsed
    print "Average time elapsed: ", g_totalTimeElapsed / numberOfRuns
    print "Min time elapsed: ", g_minTime
    print "Max time elapsed: ", g_maxTime

    if profileMemory == 1:
        print "\n:::Memory statistics::::"
        # Memory in threads
        print "Data load in the map function."
        for id, dataLoad in threadLoadMap.items():
            print "Thread: ", id, " load: ", dataLoad / numberOfRuns
            averageThreadLoadMap += dataLoad / numberOfRuns
        print "Average data load in map function: ", averageThreadLoadMap / clusterSize[clusterSizeIndex], "\n"
        print "Data load in the combine function."
        for id, dataLoad in threadLoadCombine.items():
            print "Thread: ", id, " load: ", dataLoad / numberOfRuns
            averageThreadLoadCombine += dataLoad / numberOfRuns
        print "Average data load in combine function: ", averageThreadLoadCombine / clusterSize[clusterSizeIndex], "\n"
        print "Data load in the reduce function."
        for id, dataLoad in threadLoadReduce.items():
            threadLoadReduce[id] /= reduceCounter[id]
            print "Thread: ", id, " load: ", threadLoadReduce[id]
            averageThreadLoadReduce += threadLoadReduce[id]
        print "Average data load in reduce function: ", averageThreadLoadReduce / clusterSize[clusterSizeIndex], "\n"
        # Memory in the main process
        print "Average main process memory used", totalMemory / numberOfRuns
        print "Min main process memory used", minMemory
        print "Max main process memory used", maxMemory

    print "\n:::Bandwidth statistics::::"
    totalBandwidthIn = 0
    totalBandwidthOut = 0
    for id, dataLoad in threadBandwidthIn.items():
        # threadBandwidthIn[id] /= (mapCounter[id] + numberOfRuns) # +number of runs bcs map and reduce together
        totalBandwidthIn += threadBandwidthIn[id];
        # print "Thread: ", id, " in: ", threadBandwidthIn[id]
    for id, dataLoad in threadBandwidthOut.items():
        totalBandwidthOut += threadBandwidthOut[id];
        # threadBandwidthOut[id] /= (mapCounter[id] + numberOfRuns)
        # print "Thread: ", id, " out: ", threadBandwidthOut[id]
    print "Total average bandwidth in:out (bytes) ", totalBandwidthIn / numberOfRuns, ":", totalBandwidthOut / numberOfRuns

    print ">>>>>>>>>>>>>>> END", clusterSize[clusterSizeIndex]
    # totalTimeElapsed = 0
    # minTime = sys.float_info.max
    # maxTime = 0

    if uniform == 1:
        for pool in pool:
            pool.terminate()
            pool.join()
    else:
        pool.terminate()
        pool.join()

    print "main/end"

