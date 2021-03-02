from string import split
from group import group
import datetime, time
import os
import random
import itertools


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
    t = datetime.datetime(xeval(datestr[:4]), xeval(datestr[5:7]), xeval(datestr[8:10]), xeval(datestr[11:13]),
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

    R = [1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 10000]
    # cdf job size (nodes)

    SUC = [x for x in Q_all if
           ((eval(x[IND_JOB_SIZE]) > 0) and (x[IND_STATUS] == "JOBEND") and (x[IND_START_TIME] != "")) and (
                       x[IND_END_TIME] != "")]
    m = len(SUC)  # non-zero jobs, succesfully ending
    print "m:", m
    print
    #percent of requests require r hosts
    R_CDF = [(r, len([x for x in SUC if (eval(x[IND_JOB_SIZE]) <= r)]) * 1.0 / m) for r in R]
    TSUC = [[eval(x[IND_JOB_SIZE]), parsedate(x[IND_SUBMIT_TIME]),
             parsedate(x[IND_END_TIME]) - parsedate(x[IND_START_TIME]), -1, -1, 0, 0, [], [],-1,[]] for x in SUC]

    print
    #Time from 1 to 2 ^16
    T = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 12288, 16384, 32768, 65536]
    T_CDF = [(t, len([x for x in TSUC if (x[2] <= t)]) * 1.0 / m) for t in T]
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
I_SIZE = 0 #number of hosts desired
I_ARRIVAL = 1 #arrival on trace
I_LEN = 2 # length of request time
I_SCHED = 3 #time of stating job
I_END = 4 # future time of ending job
I_OTHERS = 5
I_OTHERS_BIG = 6
I_LEAVES = 7 # number of leafs involved in mappings
I_SPINES = 8 # number of spines per mapping
I_LATENCY = 9
I_GROUPS = 10 # number of Groups in mappings


# I_DONE = 5

def checkGroupsAvaliability(mapping_groups,num_of_hosts,radix,indexes_array):
    for d in range((radix / 2)):
        sum_connectivity = 0
        for i, j in itertools.product(range(len(indexes_array)), range(len(indexes_array))):
            if (i < j):
                if mapping_groups[indexes_array[i]].longLinks[d][indexes_array[j]] == -1:
                    sum_connectivity += 1
        if sum_connectivity < len(indexes_array) -1 :
            continue
        sum = 0
        for i in range(len(indexes_array)):
            sum += mapping_groups[indexes_array[i]].get_num_Avaliable_hosts_Per_Spine(d)
        if (sum >= num_of_hosts):
            return d
    return -1


def fillGroupsWithHosts(mapping_groups,num_of_hosts,indexes_array,tennant_id,spine_num):
    #daniel is responsible
    mapping_groups_aux = [mapping_groups[x] for x in indexes_array ]
    avaliableHosts = [(group.avaliableHosts,index) for (group,index) in zip(mapping_groups_aux,indexes_array) ]
    sortedIndexes = [h[1] for h in sorted(avaliableHosts, key=lambda x: x[0])]

    allready_alocated_hosts = 0
    for i in range(len(indexes_array)-1):
        group_hosts_nums =  mapping_groups[sortedIndexes[i]].avaliableHosts
        mapping_groups[sortedIndexes[i]].fillSpineidWithHosts(spine_num, tennant_id,group_hosts_nums )
        allready_alocated_hosts += group_hosts_nums

    mapping_groups[sortedIndexes[-1]].fillSpineidWithHosts(spine_num, tennant_id, num_of_hosts-allready_alocated_hosts)

    allocated_long_link = len(indexes_array) -1
    for i, j in itertools.product(range(len(indexes_array)), range(len(indexes_array))):
        if (i < j):
            if mapping_groups[indexes_array[i]].longLinks[spine_num][indexes_array[j]] == -1:
                mapping_groups[sortedIndexes[j]].longLinks[spine_num][sortedIndexes[i]] = tennant_id
                mapping_groups[sortedIndexes[i]].longLinks[spine_num][sortedIndexes[j]] = tennant_id
                allocated_long_link -= 1

            if allocated_long_link == 0:
                break



def finalschedule(TSUC,radix):
    DC = [] #active tennants -still running
    DC_SIZE = (radix*radix/4) * (HOPS+1)   # Tentative- num of hosts of 3 Groups for now
    m = len(TSUC)
    T = [x for x in TSUC if x[0] <= DC_SIZE ]
    n = len(T)
    max_DC = 0
    print m, n, n * 1.0 / m




    # intialize all groups and their mappings
    mapping_groups = [group(radix) for i in range((radix/2) +1)]
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
        DC = [j for j in DC if T[j][I_END] > current_time] #unfinished tennanats
        dc_load = sum([T[j][I_SIZE] for j in DC]) # hosts which are taken
        DCmap = [0 for j in range(n)]
        for j in DC:
            DCmap[j] = 1

        for l in range((radix/2)+1):
            for j in range(radix/2):
             for k in range(radix/2):
                if ((mapping_groups[l].hosts[(j*radix/2)+k] != -1) and (DCmap[mapping_groups[l].hosts[(j*radix/2)+k]] == 0)):
                     mapping_groups[l].hosts[(j*radix/2)+k] = -1
                     mapping_groups[l].avaliableHosts += 1
                if ((mapping_groups[l].shortLinks[j][k] != -1) and (DCmap[mapping_groups[l].shortLinks[j][k]] == 0)):
                     mapping_groups[l].shortLinks[j][k] = -1
                if ((mapping_groups[l].longLinks[j][k] != -1) and (DCmap[mapping_groups[l].longLinks[j][k]] == 0)):
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
                dc_load = sum([T[j][I_SIZE] for j in DC])
                if (max_DC < len(DC)):
                    max_DC = len(DC)
                DCmap = [0 for j in range(n)]
                for j in DC:
                    DCmap[j] = 1

                for l in range((radix / 2) + 1):
                    for j in range(radix / 2):
                        for k in range(radix / 2):
                            if ((mapping_groups[l].hosts[(j*radix/2)+k] != -1) and (DCmap[mapping_groups[l].hosts[(j*radix/2)+k]] == 0)):
                                mapping_groups[l].hosts[(j*radix/2)+k] = -1
                                mapping_groups[l].avaliableHosts += 1
                            if ((mapping_groups[l].shortLinks[j][k] != -1) and (DCmap[mapping_groups[l].shortLinks[j][k]] == 0)):
                                mapping_groups[l].shortLinks[j][k] = -1
                            if ((mapping_groups[l].longLinks[j][k] != -1) and (DCmap[mapping_groups[l].longLinks[j][k]] == 0)):
                                mapping_groups[l].longLinks[j][k] = -1



            '''T[i][I_OTHERS] = len(DC)
            T[i][I_OTHERS_BIG] = len([x for x in DC if T[x][I_SIZE] > 1])'''


            ############### check for small###################################################
            ######################### our mapping ############################################

            avaliableHosts = [x.avaliableHosts for x in mapping_groups]
            sortedIndexes = [h[0] for h in sorted(enumerate(avaliableHosts), key=lambda x:x[1])]

            # small tennants
            if T[i][I_SIZE] <=  radix*radix/4 :
                for j in range((radix/2)+1):
                    if mapping_groups[sortedIndexes[j]].avaliableHosts >= T[i][I_SIZE] :
                        if mapping_groups[sortedIndexes[j]].is_Single_Spine_Placed(T[i][I_SIZE],i):
                            success = True
                            break


            ########################## check for big ########################################
            ############################# one Group #########################################
            ############################# now check from the biggest to the lowest ##########

            if T[i][I_SIZE] <= mapping_groups[sortedIndexes[-1]].avaliableHosts + mapping_groups[sortedIndexes[-2]].avaliableHosts + mapping_groups[sortedIndexes[-3]].avaliableHosts :
                if success == False:

                    k = radix / 2 - 1
                    for j in range((radix / 2)):
                        if mapping_groups[sortedIndexes[k-j]].avaliableHosts < T[i][I_SIZE]:
                            break
                        if mapping_groups[sortedIndexes[k-j]].is_Single_Spine_Placed(T[i][I_SIZE], i):
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
                            if j != l and frozenset([j,l]) not in forbidden:
                                if mapping_groups[sortedIndexes[k - j]].avaliableHosts + \
                                      mapping_groups[sortedIndexes[k - l]].avaliableHosts >= T[i][I_SIZE]:
                                    chosen_spine = checkGroupsAvaliability(mapping_groups, T[i][I_SIZE], radix, [sortedIndexes[k - j],sortedIndexes[k - l]])
                                    if chosen_spine != -1:
                                        fillGroupsWithHosts(mapping_groups, T[i][I_SIZE], [sortedIndexes[k - j],sortedIndexes[k - l]], i,
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
                            for m in range((radix /2)):
                                if (j != l and l != m and m != j) and frozenset([j, l, m]) not in forbidden:
                                    if mapping_groups[sortedIndexes[k - j]].avaliableHosts + \
                                            mapping_groups[sortedIndexes[k - l]].avaliableHosts + \
                                            mapping_groups[sortedIndexes[k - m]].avaliableHosts >= T[i][I_SIZE]:

                                        chosen_spine = checkGroupsAvaliability(mapping_groups, T[i][I_SIZE], radix, [sortedIndexes[k - j], sortedIndexes[k - l],sortedIndexes[k - m]])

                                        if chosen_spine != -1:
                                            fillGroupsWithHosts(mapping_groups, T[i][I_SIZE],
                                                                [sortedIndexes[k - j], sortedIndexes[k - l],sortedIndexes[k - m]], i,
                                                                chosen_spine)
                                            success = True
                                            break

                                        else:
                                            forbidden.add(frozenset([j, l,m]))

                        if success == True:
                            break


                connected= False


            T[i][I_SCHED] = current_time
            T[i][I_END] = current_time + T[i][I_LEN]
            DC.append(i)

            num_spines = 0
            num_leaves = 0
            for j in range(radix/2 + 1):
                num_leaves += mapping_groups[j].getNumOfLeavesInGroup(i)
                num_spines += mapping_groups[j].getNumOfSpinesInGroup(i)


            T[i][I_LEAVES] = num_leaves

            T[i][I_SPINES] = num_spines


            i = i + 1
    return T, max_DC



def schedule_latency(TSUC, DC_SIZE=2048):
    DC = []
    Queue = []
    last_served = -1
    FUTURE_TIME = 2000000000
    DC_SIZE_BOUND = 2048
    m = len(TSUC)
    T = [x for x in TSUC if x[0] <= DC_SIZE_BOUND]
    n = len(T)
    print m, n, n * 1.0 / m

    mapping_joint = [-1 for i in range(DC_SIZE)]
    mapping_inc = [-1 for i in range(DC_SIZE)]
    mapping_random = [-1 for i in range(DC_SIZE)]

    DCmap = [0 for i in range(n)]

    n_added = 0
    n_done = 0
    dc_load = 0
    current_time = 0

    spines_joint = []
    spines_inc = []
    spines_random = []

    i = 0
    ppp = 0
    #   while (i < 1000): 
    while (i < n):
        if (i % 1000 == 0):
            print "(", i, ")", current_time
        current_time = max(current_time, T[i][I_ARRIVAL])
        DC = [j for j in DC if T[j][I_END] > current_time]
        dc_load = sum([T[j][I_SIZE] for j in DC])
        DCmap = [0 for j in range(n)]
        for j in DC:
            DCmap[j] = 1
        for j in range(DC_SIZE):
            if ((mapping_joint[j] != -1) and (DCmap[mapping_joint[j]] == 0)):
                mapping_joint[j] = -1
            if ((mapping_inc[j] != -1) and (DCmap[mapping_inc[j]] == 0)):
                mapping_inc[j] = -1
            if ((mapping_random[j] != -1) and (DCmap[mapping_random[j]] == 0)):
                mapping_random[j] = -1

        while (T[i][I_SIZE] > (DC_SIZE - dc_load)):
            next_end = min([T[j][I_END] for j in DC])
            if (next_end < current_time):
                print "please check"
            current_time = next_end
            DC = [j for j in DC if T[j][I_END] > current_time]
            dc_load = sum([T[j][I_SIZE] for j in DC])

            DCmap = [0 for j in range(n)]
            for j in DC:
                DCmap[j] = 1
            for j in range(DC_SIZE):
                if ((mapping_joint[j] != -1) and (DCmap[mapping_joint[j]] == 0)):
                    mapping_joint[j] = -1
                if ((mapping_inc[j] != -1) and (DCmap[mapping_inc[j]] == 0)):
                    mapping_inc[j] = -1
                if ((mapping_random[j] != -1) and (DCmap[mapping_random[j]] == 0)):
                    mapping_random[j] = -1

        T[i][I_SCHED] = current_time
        T[i][I_END] = current_time + T[i][I_LEN]

        DC.append(i)
        T[i][I_OTHERS] = len(DC)
        T[i][I_OTHERS_BIG] = len([x for x in DC if T[x][I_SIZE] > 1])
        dc_load = sum([T[j][I_SIZE] for j in DC])

        mapping_joint = [-1 for j in range(DC_SIZE)]
        last = 0
        for x in DC:
            for j in range(T[x][I_SIZE]):
                mapping_joint[last + j] = x
            last = last + T[x][I_SIZE]

        sets = 0
        j = 0
        while ((sets < T[i][I_SIZE]) and (j < DC_SIZE)):
            if (mapping_inc[j] == -1):
                mapping_inc[j] = i
                sets = sets + 1
            j = j + 1

        sets = 0
        j = random.randint(0, DC_SIZE - 1)
        while (sets < T[i][I_SIZE]):
            if (mapping_random[j] == -1):
                mapping_random[j] = i
                sets = sets + 1
                j = random.randint(0, DC_SIZE - 1)
            j = (j + 1) % DC_SIZE

        grps_joint = 0
        grps_inc = 0
        grps_random = 0
        LEAF_SIZE = 32
        for j in range(DC_SIZE / LEAF_SIZE):
            found_joint = 0
            found_inc = 0
            found_random = 0
            for k in range(32):
                if (mapping_joint[j * 32 + k] == i):
                    found_joint = 1
                if (mapping_inc[j * 32 + k] == i):
                    found_inc = 1
                if (mapping_random[j * 32 + k] == i):
                    found_random = 1
            grps_joint += found_joint
            grps_inc += found_inc
            grps_random += found_random
        '''
        T[i][I_LEAVES] = [grps_joint, grps_inc, grps_random]
        T[i][I_SPINES] = [spineformapping(mapping_joint, DC_SIZE / LEAF_SIZE),
                          spineformapping(mapping_inc, DC_SIZE / LEAF_SIZE),
                          spineformapping(mapping_random, DC_SIZE / LEAF_SIZE)]
        if (spineformapping(mapping_joint) > 2) and (ppp == 0):
            print "@", mapping_joint
            ppp = 1
        '''
        i = i + 1
    return T


def schedule_latency_short(TSUC, DC_SIZE=2048):
    DC = []
    Queue = []
    last_served = -1
    FUTURE_TIME = 2000000000
    DC_SIZE_BOUND = 2048
    m = len(TSUC)
    T = [x for x in TSUC if x[0] <= DC_SIZE_BOUND]
    n = len(T)
    print m, n, n * 1.0 / m

    mapping_joint = [-1 for i in range(DC_SIZE)]
    mapping_inc = [-1 for i in range(DC_SIZE)]
    mapping_random = [-1 for i in range(DC_SIZE)]

    DCmap = [0 for i in range(n)]

    n_added = 0
    n_done = 0
    dc_load = 0
    current_time = 0

    spines_joint = []
    spines_inc = []
    spines_random = []

    i = 0
    ppp = 0
    #    while (i < 3000): 
    while (i < n):
        if (i % 1000 == 0):
            print "(", i, ")", current_time
        current_time = max(current_time, T[i][I_ARRIVAL])
        DC = [j for j in DC if T[j][I_END] > current_time]
        dc_load = sum([T[j][I_SIZE] for j in DC])
        DCmap = [0 for j in range(n)]
        for j in DC:
            DCmap[j] = 1
        for j in range(DC_SIZE):
            if ((mapping_joint[j] != -1) and (DCmap[mapping_joint[j]] == 0)):
                mapping_joint[j] = -1
            if ((mapping_inc[j] != -1) and (DCmap[mapping_inc[j]] == 0)):
                mapping_inc[j] = -1
            if ((mapping_random[j] != -1) and (DCmap[mapping_random[j]] == 0)):
                mapping_random[j] = -1

        while (T[i][I_SIZE] > (DC_SIZE - dc_load)):
            next_end = min([T[j][I_END] for j in DC])
            if (next_end < current_time):
                print "please check"
            current_time = next_end
            DC = [j for j in DC if T[j][I_END] > current_time]
            dc_load = sum([T[j][I_SIZE] for j in DC])

            DCmap = [0 for j in range(n)]
            for j in DC:
                DCmap[j] = 1
            for j in range(DC_SIZE):
                if ((mapping_joint[j] != -1) and (DCmap[mapping_joint[j]] == 0)):
                    mapping_joint[j] = -1
                if ((mapping_inc[j] != -1) and (DCmap[mapping_inc[j]] == 0)):
                    mapping_inc[j] = -1
                if ((mapping_random[j] != -1) and (DCmap[mapping_random[j]] == 0)):
                    mapping_random[j] = -1

        T[i][I_SCHED] = current_time
        T[i][I_END] = current_time + T[i][I_LEN]
        T[i][I_LATENCY] = T[i][I_SCHED] - T[i][I_ARRIVAL]

        DC.append(i)
        T[i][I_OTHERS] = len(DC)
        T[i][I_OTHERS_BIG] = len([x for x in DC if T[x][I_SIZE] > 1])
        dc_load = sum([T[j][I_SIZE] for j in DC])

        i = i + 1
    return T


def main():
    TSUC = read()
    T,max_DC = finalschedule(TSUC,16)
    print("success")
    print(max_DC)
    return 3

    print
    print "Parallel"
    parallel_vals = [2 ** i for i in range(0, 12)]

    others = [len([i for i in range(len(T)) if T[i][I_OTHERS] <= bound]) for bound in parallel_vals]
    others_big = [len([i for i in range(len(T)) if T[i][I_OTHERS_BIG] <= bound]) for bound in parallel_vals]

    print "big tenants:"
    for i in range(len(parallel_vals)):
        print "(", 2 ** i, ",", others_big[i] * 1.0 / len(T), ")"

    print
    print "$", sum([T[i][I_OTHERS_BIG] for i in range(len(T))]) * 1.0 / len(T)
    print

    print "all tenants:"
    for i in range(len(parallel_vals)):
        print "(", 2 ** i, ",", others[i] * 1.0 / len(T), ")"
    print
    print "$", sum([T[i][I_OTHERS] for i in range(len(T))]) * 1.0 / len(T)
    print
    print
    leaves_vals = [2 ** i for i in range(0, 7)]
    spine_vals = [1, 2, 4, 8, 16, 24, 32, 48]

    leaves = [[len([i for i in range(len(T)) if T[i][I_LEAVES][j] <= bound]) for bound in leaves_vals] for j in
              range(3)]
    spines = [[len([i for i in range(len(T)) if T[i][I_SPINES][j] <= bound]) for bound in spine_vals] for j in range(3)]

    print "leaves"
    for j in range(3):
        for i in range(len(leaves_vals)):
            print "(", leaves_vals[i], ",", leaves[j][i] * 1.0 / len(T), ")"
        print
    print
    print "$", [sum([T[i][I_LEAVES][j] for i in range(len(T))]) * 1.0 / len(T) for j in range(3)]

    print "spines"
    for j in range(3):
        for i in range(len(spine_vals)):
            print "(", spine_vals[i], ",", spines[j][i] * 1.0 / len(T), ")"
        print
    print
    print "$", [sum([T[i][I_SPINES][j] for i in range(len(T))]) * 1.0 / len(T) for j in range(3)]


def latency_test():
    TSUC = read()

    d = 10
    T_LEN = 16435
    Td = [int(round(T_LEN * i * 1.0 / d)) for i in range(d + 1)]

    DC_SIZE_VALS = [16384]  # [2048, 4096, 8192, 16384]
    LATENCY_BOUND = 7000000
    Ld = [0, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 30000, 40000, 50000, 75000]
    for x in range(100000, LATENCY_BOUND + 1, 200000):
        Ld.append(x)

    for DC_SIZE in DC_SIZE_VALS:
        print "DC_SIZE: ", DC_SIZE
        T = schedule_latency_short(TSUC, DC_SIZE)

        print "$", sum([(T[i][I_SCHED] - T[i][I_ARRIVAL]) for i in range(len(T))]) * 1.0 / len(T),
        print len([i for i in range(len(T)) if T[i][I_SCHED] == T[i][I_ARRIVAL]])
        print "!!!", len([i for i in range(len(T)) if T[i][I_SCHED] < T[i][I_ARRIVAL]]), len(
            [i for i in range(len(T)) if T[i][I_LATENCY < 0]])

        CDF = [(L, len([i for i in range(len(T)) if T[i][I_LATENCY] <= L]) * 1.0 / T_LEN) for L in Ld]
        # for x in CDF:
        #    print x
        print "&", "(d:", d, ")   ",
        for i in range(d):
            Q = T[Td[i]:Td[i + 1]][:]
            print sum([x[I_LATENCY] for x in Q]) * 1.0 / len(Q),

    print

    return

if __name__ == '__main__':
    main()