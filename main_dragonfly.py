import pickle
from string import split
from group import group
import datetime, time
import os
import random
import itertools
import numpy as np
import matplotlib as plt

#


# os.chdir("DisjointTreesACM/Data/Trinity")
fname = 'Trinity.csv'

HOPS = 2

IND_START_TIME = 3
IND_END_TIME = 6
IND_STATUS = 8
IND_JOB_SIZE = 9
IND_SUBMIT_TIME = 2
"""
Fields:
0 - 'user_ID',
1 - 'group_ID',/Users/orir/Downloads/DisjointTrees.py
2 - 'submit_time',
3 - 'start_time',
4 - 'dispatch_time',
5 - 'queue_time',
6 - 'end_time',
7 - 'wallclock_limit',
8 - 'job_status',
9 - 'node_count',
10 - 'tasks_requested'
"""


def xeval(l):
    x = l[:]
    while ((len(x) > 1) and (x[0] == "0")):
        x = x[1:]
    return eval(x)


def parsedate(datestr):
    t = datetime.datetime(xeval(datestr[:4]), xeval(datestr[5:7]),
                          xeval(datestr[8:10]), xeval(datestr[11:13]),
                          xeval(datestr[14:16]), xeval(datestr[17:19]))
    return time.mktime(t.timetuple())


def read():
    fname = 'Trinity.csv'
    l = open(fname).readlines()
    sl = [split(x, ",") for x in l]
    Q_all = sl[1:]
    n = len(Q_all)  # all jobs

    # reading the node-count field    
    Q = [eval(x[IND_JOB_SIZE]) for x in Q_all if eval(x[IND_JOB_SIZE]) > 0]

    R = [1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024, 2048,
         4096, 8192, 10000]
    # cdf job size (nodes)

    SUC = [x for x in Q_all if
           ((eval(x[IND_JOB_SIZE]) > 0) and (
                   x[IND_STATUS] == "JOBEND") and (
                    x[IND_START_TIME] != "")) and (
                   x[IND_END_TIME] != "")]
    m = len(SUC)  # non-zero jobs, succesfully ending
    print "m:", m
    print
    # percent of requests require r hosts
    R_CDF = [(r, len(
        [x for x in SUC if (eval(x[IND_JOB_SIZE]) <= r)]) * 1.0 / m) for r
             in R]
    TSUC = [[eval(x[IND_JOB_SIZE]), parsedate(x[IND_SUBMIT_TIME]),
             parsedate(x[IND_END_TIME]) - parsedate(x[IND_START_TIME]), -1,
             -1, 0, 0, [], [], -1, []] for x in SUC]

    print
    # Time from 1 to 2 ^16
    T = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
         12288, 16384, 32768, 65536]
    T_CDF = [(t, len([x for x in TSUC if (x[2] <= t)]) * 1.0 / m) for t in
             T]
    # for x in T_CDF:
    #    print x

    print "done read B"
    return TSUC


"""
0 JOB_SIZE
1 ARRIVAL_TIME
2 LENGTH
3 scheduled_time
4 end_time
5 done
"""
I_SIZE = 0  # number of hosts desired
I_ARRIVAL = 1  # arrival on trace
I_LEN = 2  # length of request time
I_SCHED = 3  # time of stating job
I_END = 4  # future time of ending job
I_OTHERS = 5
I_OTHERS_BIG = 6
I_LEAVES = 7  # number of leafs involved in mappings
I_SPINES = 8  # number of spines per mapping
I_LATENCY = 9
I_GROUPS = 10  # number of Groups in mappings
I_PARALLEL = 11
I_PARALLEL_BIG = 12


# I_DONE = 5

def checkGroupsAvaliability(mapping_groups, num_of_hosts, radix,
                            indexes_array):
    for d in range((radix / 2)):
        sum_connectivity = 0
        for i, j in itertools.product(range(len(indexes_array)),
                                      range(len(indexes_array))):
            if (i < j):
                if mapping_groups[indexes_array[i]].longLinks[d][
                    indexes_array[j]] == -1:
                    sum_connectivity += 1
        if sum_connectivity < len(indexes_array) - 1:
            continue
        sum = 0
        for i in range(len(indexes_array)):
            sum += mapping_groups[
                indexes_array[i]].get_num_Avaliable_hosts_Per_Spine(d)
        if (sum >= num_of_hosts):
            return d
    return -1


def fillGroupsWithHosts(mapping_groups, num_of_hosts, indexes_array,
                        tennant_id, spine_num):
    # daniel is responsible
    mapping_groups_aux = [mapping_groups[x] for x in indexes_array]
    avaliableHosts = [(group.avaliableHosts, index) for (group, index) in
                      zip(mapping_groups_aux, indexes_array)]
    sortedIndexes = [h[1] for h in
                     sorted(avaliableHosts, key=lambda x: x[0])]

    allready_alocated_hosts = 0
    for i in range(len(indexes_array) - 1):
        group_hosts_nums = mapping_groups[sortedIndexes[i]].avaliableHosts
        mapping_groups[sortedIndexes[i]].fillSpineidWithHosts(spine_num,
                                                              tennant_id,
                                                              group_hosts_nums)
        allready_alocated_hosts += group_hosts_nums

    mapping_groups[sortedIndexes[-1]].fillSpineidWithHosts(spine_num,
                                                           tennant_id,
                                                           num_of_hosts - allready_alocated_hosts)

    allocated_long_link = len(indexes_array) - 1
    for i, j in itertools.product(range(len(indexes_array)),
                                  range(len(indexes_array))):
        if (i < j):
            if mapping_groups[indexes_array[i]].longLinks[spine_num][
                indexes_array[j]] == -1:
                mapping_groups[sortedIndexes[j]].longLinks[spine_num][
                    sortedIndexes[i]] = tennant_id
                mapping_groups[sortedIndexes[i]].longLinks[spine_num][
                    sortedIndexes[j]] = tennant_id
                allocated_long_link -= 1

            if allocated_long_link == 0:
                break


def finalSchedule(TSUC, radix):
    DC = []  # active tennants -still running
    DC_SIZE = (radix * radix / 4) * (
            HOPS + 1)  # Tentative- num of hosts of 3 Groups for now
    m = len(TSUC)
    T = [x for x in TSUC if x[0] <= DC_SIZE]
    print 'T size after filter %s', len(T)
    print 'T size before filter %s', m
    print m / len(T)
    for e in range(len(T)):
        T[e].append(-1)
        T[e].append(-1)
    n = len(T)
    max_DC = 0
    print m, n, n * 1.0 / m
    dc_parallel_big = []
    max_latency = -1

    # intialize all groups and their mappings
    mapping_groups = [group(radix) for i in range((radix / 2) + 1)]
    current_time = 0

    '''
    DC - all the tennant indexs that are still running and take hosts 
    DCmap- an array of all tennats that determines which tennat is still running- (1- still running, 0- finished)
    mapping arrays- all the hosts that are taken or not in the array- a values is a index of a tennat which is holding the host 
    
    '''

    i = 0

    while (i < n):
        success = False
        if (i % 1000 == 0):
            print "(", i, ")", current_time
        current_time = max(current_time, T[i][I_ARRIVAL])
        DC = [j for j in DC if
              T[j][I_END] > current_time]  # unfinished tennanats
        dc_load = sum([T[j][I_SIZE] for j in DC])  # hosts which are taken
        DCmap = [0 for j in range(n)]
        for j in DC:
            DCmap[j] = 1

        for l in range((radix / 2) + 1):
            for j in range(radix / 2):
                for k in range(radix / 2):
                    if ((mapping_groups[l].hosts[
                             (j * radix / 2) + k] != -1) and (DCmap[
                                                                  mapping_groups[
                                                                      l].hosts[
                                                                      (
                                                                              j * radix / 2) + k]] == 0)):
                        mapping_groups[l].hosts[(j * radix / 2) + k] = -1
                        mapping_groups[l].avaliableHosts += 1
                    if ((mapping_groups[l].shortLinks[j][k] != -1) and (
                            DCmap[
                                mapping_groups[l].shortLinks[j][k]] == 0)):
                        mapping_groups[l].shortLinks[j][k] = -1
                    if ((mapping_groups[l].longLinks[j][k] != -1) and (
                            DCmap[
                                mapping_groups[l].longLinks[j][k]] == 0)):
                        mapping_groups[l].longLinks[j][k] = -1

        connected = True
        while success == False:

            while ((T[i][I_SIZE] > (DC_SIZE - dc_load)) or not connected):
                connected = True
                next_end = min([T[j][I_END] for j in DC])
                if (next_end < current_time):
                    print "please check"
                current_time = next_end
                DC = [j for j in DC if T[j][I_END] > current_time]
                dc_parallel_big = [j for j in DC if
                                   T[j][I_END] > current_time and T[j][
                                       I_SIZE] > 1]

                dc_load = sum([T[j][I_SIZE] for j in DC])
                if (max_DC < len(dc_parallel_big)):
                    max_DC = len(dc_parallel_big)

                DCmap = [0 for j in range(n)]
                for j in DC:
                    DCmap[j] = 1

                for l in range((radix / 2) + 1):
                    for j in range(radix / 2):
                        for k in range(radix / 2):
                            if ((mapping_groups[l].hosts[
                                     (j * radix / 2) + k] != -1) and (
                                    DCmap[mapping_groups[l].hosts[
                                        (j * radix / 2) + k]] == 0)):
                                mapping_groups[l].hosts[
                                    (j * radix / 2) + k] = -1
                                mapping_groups[l].avaliableHosts += 1
                            if ((mapping_groups[l].shortLinks[j][
                                     k] != -1) and (DCmap[mapping_groups[
                                l].shortLinks[j][k]] == 0)):
                                mapping_groups[l].shortLinks[j][k] = -1
                            if ((mapping_groups[l].longLinks[j][
                                     k] != -1) and (DCmap[mapping_groups[
                                l].longLinks[j][k]] == 0)):
                                mapping_groups[l].longLinks[j][k] = -1

            '''T[i][I_OTHERS] = len(DC)
            T[i][I_OTHERS_BIG] = len([x for x in DC if T[x][I_SIZE] > 1])'''

            ############### check for small###################################################
            ######################### our mapping ############################################

            avaliableHosts = [x.avaliableHosts for x in mapping_groups]
            sortedIndexes = [h[0] for h in
                             sorted(enumerate(avaliableHosts),
                                    key=lambda x: x[1])]

            # small tennants
            if T[i][I_SIZE] <= radix * radix / 4:
                for j in range((radix / 2) + 1):
                    if mapping_groups[sortedIndexes[j]].avaliableHosts >= \
                            T[i][I_SIZE]:
                        if mapping_groups[
                            sortedIndexes[j]].is_Single_Spine_Placed(
                            T[i][I_SIZE], i):
                            success = True
                            break

            ########################## check for big ########################################
            ############################# one Group #########################################
            ############################# now check from the biggest to the lowest ##########

            if T[i][I_SIZE] <= mapping_groups[
                sortedIndexes[-1]].avaliableHosts + mapping_groups[
                sortedIndexes[-2]].avaliableHosts + mapping_groups[
                sortedIndexes[-3]].avaliableHosts:
                if success == False:

                    k = radix / 2 - 1
                    for j in range((radix / 2)):
                        if mapping_groups[
                            sortedIndexes[k - j]].avaliableHosts < T[i][
                            I_SIZE]:
                            break
                        if mapping_groups[
                            sortedIndexes[k - j]].is_Single_Spine_Placed(
                            T[i][I_SIZE], i):
                            success = True
                            break

                ######################### Two Groups ##########################################

                ## check that both groups never appered together - forbiden sets
                ## check two spines in a for loop every time and if long link avaliable - if there hosts is greater than
                ## needed, fill the smaller one and the rest fill with the greater one
                ## sub finction- get size of hosts per spine in a group
                ## if failed - add to the forbidden subsets

                if success == False:
                    forbidden = {}
                    k = radix / 2 - 1
                    for j in range((radix / 2)):
                        for l in range((radix / 2)):
                            if j != l and frozenset(
                                    [j, l]) not in forbidden:
                                if mapping_groups[
                                    sortedIndexes[k - j]].avaliableHosts + \
                                        mapping_groups[sortedIndexes[
                                            k - l]].avaliableHosts >= T[i][
                                    I_SIZE]:
                                    chosen_spine = checkGroupsAvaliability(
                                        mapping_groups, T[i][I_SIZE],
                                        radix, [sortedIndexes[k - j],
                                                sortedIndexes[k - l]])
                                    if chosen_spine != -1:
                                        fillGroupsWithHosts(mapping_groups,
                                                            T[i][I_SIZE], [
                                                                sortedIndexes[
                                                                    k - j],
                                                                sortedIndexes[
                                                                    k - l]],
                                                            i,
                                                            chosen_spine)
                                        success = True
                                        break

                                    else:
                                        forbidden.add(frozenset([j, l]))

                        if success == True:
                            break

                ######################### three groups ########################################
                if success == False:
                    forbidden = {}
                    k = radix / 2 - 1
                    for j in range((radix / 2)):
                        for l in range((radix / 2)):
                            for m in range((radix / 2)):
                                if (
                                        j != l and l != m and m != j) and frozenset(
                                    [j, l, m]) not in forbidden:
                                    if mapping_groups[sortedIndexes[
                                        k - j]].avaliableHosts + \
                                            mapping_groups[sortedIndexes[
                                                k - l]].avaliableHosts + \
                                            mapping_groups[sortedIndexes[
                                                k - m]].avaliableHosts >= \
                                            T[i][I_SIZE]:

                                        chosen_spine = checkGroupsAvaliability(
                                            mapping_groups, T[i][I_SIZE],
                                            radix, [sortedIndexes[k - j],
                                                    sortedIndexes[k - l],
                                                    sortedIndexes[k - m]])

                                        if chosen_spine != -1:
                                            fillGroupsWithHosts(
                                                mapping_groups,
                                                T[i][I_SIZE],
                                                [sortedIndexes[k - j],
                                                 sortedIndexes[k - l],
                                                 sortedIndexes[k - m]], i,
                                                chosen_spine)
                                            success = True
                                            break

                                        else:
                                            forbidden.add(
                                                frozenset([j, l, m]))

                        if success == True:
                            break

                connected = False

            T[i][I_SCHED] = current_time
            T[i][I_END] = current_time + T[i][I_LEN]
            DC.append(i)

            num_spines = 0
            num_leaves = 0
            for j in range(radix / 2 + 1):
                num_leaves += mapping_groups[j].getNumOfLeavesInGroup(i)
                num_spines += mapping_groups[j].getNumOfSpinesInGroup(i)

            T[i][I_LEAVES] = num_leaves

            T[i][I_SPINES] = num_spines

            T[i][I_PARALLEL] = len(DC)
            T[i][I_PARALLEL_BIG] = len(dc_parallel_big)
            T[i][I_LATENCY] = T[i][I_SCHED] - T[i][I_ARRIVAL]

            i = i + 1
    return T, max_DC


def main(radix):
    """
    runi
    """
    TSUC = read()
    for i, e in enumerate(TSUC):
        TSUC[i][I_GROUPS] = -1
    T, max_DC = finalSchedule(TSUC, radix)
    print("success")
    print(max_DC)
    print "saveing result"
    save_as_text(T, "sched_%s.csv" % radix)
    print


def save_as_text(T, label):
    # T = np.delete(np.array(T), -3, 1)
    A = np.array([np.array(xi) for i, xi in zip(range(len(T)), T)])
    # A=np.delete(A,-1,axis=1)
    np.savetxt(label, A, fmt='%s')


def get_leaves_data(radix):
    """this function gathering data regarding the the leaves tenants
    NOTE: make sure you have the file sched_.<radix>.csv
    the output is leaves_data_<radix>.pickle that
     can be used for drawing graphs
    """

    if os.path.exists('sched_%s.csv' % radix) != True:
        print 'no such file sched_%s.csv' % radix
        return

    T = np.loadtxt('sched_%s.csv' % radix, dtype=int)
    leaves_vals = [2 ** i for i in range(0, 7)]

    leaves = [len([i for i in range(len(T)) if T[i][I_LEAVES] <= bound])
              for bound in leaves_vals]

    print "leafs are ", leaves
    leaves = [leaves[i] * 1.0 / len(T) for i in range(len(leaves_vals))]

    print "leafs after transformation are ", leaves
    print "leafs vals are ", leaves_vals
    merged_list = [(leaves_vals[i], leaves[i]) for i in
                   range(0, len(leaves_vals))]

    with open("leaves_data_%s.pickle" % radix, 'w+') as handle:
        pickle.dump(merged_list, handle)


def get_latency_data():
    """
    this function is for running latency tests. you can change RADIX_SIZES
    and LATENCY_BOUND for more data as you wish
    the output is latency_data<radix>.pickle, where <radix> is every value
    in RADIX_SIZES
    """
    TSUC = read()

    d = 10

    RADIX_SIZES = [24, 32, 40, 50]  # [2048, 4096, 8192, 16384]
    LATENCY_BOUND = 8000000
    Ld = [0, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000,
          20000, 30000, 40000, 50000, 75000]
    for x in range(100000, LATENCY_BOUND + 1, 200000):
        Ld.append(x)

    for RADIX in RADIX_SIZES:
        UPPER_BOUND = (RADIX * RADIX / 4) * (
                HOPS + 1)

        print "DC_SIZE: ", RADIX
        # run in radix sizes
        T, max_DC = finalSchedule(TSUC, RADIX)
        T_LEN = len([x for x in T if x[0] <= UPPER_BOUND])
        assert T_LEN == len(T)
        CDF = [(L, len([i for i in range(len(T))
                        if T[i][I_LATENCY] <= L]) * 1.0 / T_LEN) for L in
               Ld]
        with open('latency_data%s.pickle' % RADIX, 'w+') as handler:
            pickle.dump(CDF, handler)
        print 'cdf is'
        print CDF
        print "&", "(d:", d, ")   ",

    print

    return


def get_parallel(T, radix):
    """util function that collects data regarding the parallel tenants """
    parallel_vals = [2 ** i for i in range(0, 12)]

    others = [
        len([i for i in range(len(T)) if T[i][I_PARALLEL] <= bound])
        for bound in parallel_vals]

    others_big = [
        len([i for i in range(len(T)) if
             T[i][I_PARALLEL_BIG] <= bound]) for
        bound in parallel_vals]
    print "big tenants:"
    big_tenant = []
    for i in range(len(parallel_vals)):
        if i > len(others_big) - 1:
            break
        print "(", 2 ** i, ",", others_big[i] * 1.0 / len(T), ")"
        big_tenant.append((2 ** i, others_big[i] * 1.0 / len(T)))

    print
    with open("big_parallel_graph_%s.pickle" % radix, 'w+') as handle:
        pickle.dump(big_tenant, handle)

    print "$", sum(
        [T[i][I_PARALLEL] for i in range(len(T))]) * 1.0 / len(T)
    print

    all_tenant = []
    print "all tenants:"
    for i in range(len(parallel_vals)):
        if i > len(others) - 1:
            break
        print "(", 2 ** i, ",", others[i] * 1.0 / len(T), ")"
        all_tenant.append((2 ** i, others[i] * 1.0 / len(T)))
    print
    with open("all_parallel_graph_%s.pickle" % radix, 'w+') as handle:
        pickle.dump(all_tenant, handle)

    print "$", sum(
        [T[i][I_PARALLEL] for i in range(len(T))]) * 1.0 / len(T)
    print
    print


def get_parallel_data(radix):
    """this function gathering data regarding the parallel tenants
     NOTE: make sure you have the file sched_.<radix>.csv
     the output is big_parallel_graph<radix>.pickle  and
        all_parallel_graph<radix>.pickle that
        can be used for drawing graphs
     """

    if os.path.exists('sched_%s.csv' % radix) != True:
        print 'no such file sched_%s.csv' % radix
        return
    T = np.loadtxt('sched_%s.csv' % radix, dtype=int)
    get_parallel(T, radix)


def draw_latency_graph(radix):
    if os.path.exists('latency_data%s.pickle'% radix) != True:
        print 'no such file sched_%s.csv' % radix
        return
    with open('latency_data%s.pickle'% radix, 'rb') as handle:
        b = pickle.load(handle)

    plt.plot([y for x, y in b], [x for x, y in b])
    plt.xlabel("leafs")
    plt.ylabel("CDF")
    plt.legend()


if __name__ == '__main__':
    radix=24
    main(radix)
    get_leaves_data(radix)
    get_latency_data()
    get_parallel_data(radix)
