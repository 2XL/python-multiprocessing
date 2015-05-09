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
import memory_profiler
import pdb
import py_ecc
import random
import multiprocessing

import logging
import math

import time
from functools import wraps
from guppy import hpy


def fn_timer(function):
    @wraps(function)
    def function_timer(*args, **kwargs):
        t0 = time.time()
        result = function(*args, **kwargs)
        t1 = time.time()
        print ("Total time running %s: %s seconds" %
               (function.func_name, str(t1 - t0))
        )
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


@fn_timer
def map_reduce(distributionType, clusterSize, testRun, logData, outputPath):
    """
    @distributionType: boolean kind of distribution
    @clusterSize: int number of nodes
    @testRun: int number of runs
    @logData: list of log
    @outputPath: string path to output folder

    """
    print "map_reduce/Start! ",
    print distributionType, clusterSize, testRun, outputPath

    # multi... GIL (global interpreter lock)
    # python multiprocessing module
    # python Pool.provides map
    #
    # [IP[0]      YEAR[5]        MONTH[4]       N#VISITS]
    # INPUT
    line = logData[0]
    print str(line[0]) + "\t" + str(line[1]) + "\t" + str(line[2]) + "\t" + "###";

    # build a pool of @clusterSize
    pool = multiprocessing.Pool(processes=clusterSize, )

    # Fragment the input log into @clusterSize chunks
    logLines = len(logData)
    partitionLength = clusterSize;
    logChunkSize = int(math.ceil(logLines / partitionLength))

    print str(logLines) + " into chunks of size: " + str(logChunkSize)
    list = [x for x in xrange(0, len(logData) + 1, logChunkSize)]
    list[-1] = logLines  # fix the last offset
    # SPLIT
    logChunkList = lindexsplit(logData, list)

    # Fetch map operations
    # MAP
    map_visitor = pool.map(Map, logChunkList)
    # print map_visitor

    print map_visitor

    # COMBINER
    # TODO

    # Organize the mapped output
    # SHUTTLE/SORT
    combiner_visitor = Partition(map_visitor)
    print combiner_visitor

    #print combiner_visitor.items()
    #print len(combiner_visitor.items())
    # parse items into sets of 4 ??? o ja ho fa automaticament?
    # Refector additional step
    # REDUCE
    visitor_frequency = pool.map(Reduce, combiner_visitor.items())

    # OUTPUT
    print "OUTPUT",
    print visitor_frequency
    # Sort in some order
    # frequency_rank = visitor_frequency.sort(tuple_sort)


    # Display TOP
    #for pair in frequency_rank[:5]:
    #    print pair[0], ": ", pair[1]


    # queda afegir els terminates & joins

    print "map_reduce/Finish!"


"""
Map
num visits x month group by month, unique IP
1 map visit by ip, {num}
print str(ip[0])+"\t"+ str(year[5])+ "\t"+str(month[4])+"\t"+"###";
"""


@fn_timer
def Map(L):
    # print "Map:", multiprocessing.current_process().name, "\t",
    #print len(L)
    results = {}  # key value storage
    for line in L:
        key = str(line[0] +":" + line[1] +":" + line[2]);
        try:

            results[key] += 1
        except KeyError:
            results[key] = 1

    # print line[4]
    return results


"""
Partition
3 merge and order by
"""


@fn_timer
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


"""
Combiner
2 map visit by month, {ip}
# http://moodle.urv.cat/moodle/pluginfile.php/1942405/mod_resource/content/1/ADS15%20-%20MapReduce%20Programming.pdf
"""


@fn_timer
def Combiner(L):
    results = {}  # key value storage
    for line in L:
        try:
            results[str(line[2])] += 1
        except KeyError:
            results[str(line[2])] = 1

            # print line[4]
    return results
"""
Reduce
num visits x month group by month, unique IP
IP YEAR MONTH num
"""


@fn_timer
def Reduce(Mapping):
    # print "Reduce"
    return Mapping[0], sum(pair for pair in Mapping[1])


"""

Load the contents the file at the given path into a big string and return it as a list of lists

--
>>cpu time
pip install line_profiler
>kernprof.py -l -v test.py
pip install -U memory_profiler
>>memory usage
>python -m memory_profiler test.py
pip install psutil
>>memory leak
>pip install objgraph
pdb.set_trace()
debuggnig
>>memory usage | type
>pip install guppy


@profile
gr8 tool to seek bottleneck, but got conflict with Pool
"""


@fn_timer
def load(path):
    print "load/" + path
    hp = hpy()
    print "Heap at the beginning of the function\n", hp.heap()
    file_rows = []
    row = []
    f = open(path, "r")
    for line in f:
        row = re.split(r'\t+', line.rstrip('\t'))
        file_rows.append([row[0],row[5],row[4]])
    # add try catch handle error???
    # pdb.set_trace()
    print "Heap at the end of the function\n", hp.heap()
    return file_rows


"""
Magic tuple sorting by ...
"""


@fn_timer
def tuple_sort(a, b):
    if a[1] < b[1]:
        return 1
    elif a[1] > b[1]:
        return -1
    else:
        return cmp(a[0], b[0])


"""
Partition the loglist
"""


@fn_timer
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


if __name__ == "__main__":

    if (len(sys.argv) != 1):
        print "Program arguments...";
        print sys.argv
        sys.exit(1);

    print "main/start:"

    # load file

    print "TODO"
    # with py_performance.timer.Timer() as t:
    logFile = load("file/logs_min.txt")
    #print "=> elapsed loadFile: %s s" %t.secs

    numScenarios = 6;
    clusterSize = [4, 8, 16]  # nodes
    testRunsRandom = 100  # num of test iterations
    # random distribution --> test_pool_who_i_am
    testRunsUniform = 100
    # uniform distribution --> test_pool_who_i_am_uniform

    # hint:
    # apply each function to Pool.map()

    # apply refactoring to Pool.combiner()

    # apply result to Pool.reduce()

    # evaluate:
    #
    # execution time
    # avg
    # min
    # max
    #
    # memory usage
    # avg
    # min
    # max
    #
    # bandwidth consumption (bytes, worker <-> main)
    #
    # avg
    # min
    # max
    #
    # evaluate extra:
    # improving the speedup
    # memory usage --> use the correct attributes from the list --> just timestamp or 2 atributes
    # bandwidth consumption --> add a combiner
    # cpu time --> fast and furious ... RIP
    #
    # 1 cpu time
    # 2 bootleneck, initial file reading...
    # 3 memory usage
    # 4 memory leak ?
    #

    #with py_performance.timer.Timer() as t:
    map_reduce(False, clusterSize[2], testRunsRandom, logFile, "file/out/");
    #print "=> elapsed map_reduce: %s s" %t.secs
    print "main/end"



