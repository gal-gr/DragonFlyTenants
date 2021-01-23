from string import split
from group import group
import datetime, time
import os
import random

#


# os.chdir("DisjointTreesACM/Data/Trinity")
fname = 'Trinity.csv'

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


def spineformapping(mapping, leafs=64):  # a mapping is a vector of size DC_SIZE
    leaf_size = 32
    tenantsD = {}
    for x in mapping:
        if x != -1:
            tenantsD[x] = [0 for j in range(leafs)]

    # computing the groups for each existing tenant
    tenantsIDs = tenantsD.keys()
    for j in range(leafs):
        for k in range(32):
            x = mapping[j * 32 + k]
            if (x != -1):
                tenantsD[x][j] = 1

    for x in tenantsIDs:
        if (sum(tenantsD[x]) == 1):
            tenantsD.pop(x)
    tenantsIDs = tenantsD.keys()
    tenantsIDs.sort()
    spines = []
    #    print tenantsIDs
    for ten in tenantsIDs:
        tenant = tenantsD[ten]
        n_spines = len(spines)
        found = 0
        s = 0
        while ((found == 0) and (s < n_spines)):
            legal = 1
            for j in range(leafs):
                if ((tenant[j] == 1) and (spines[s][j] == 1)):
                    legal = 0
            if (legal):
                found = 1
                for j in range(leafs):
                    if (tenant[j]):
                        spines[s][j] = 1
            else:
                s = s + 1
        if found == 0:
            newspine = tenant[:]
            spines.append(newspine)

    return len(spines)


def finalschedule(TSUC,radix):
    DC = [] #active tennants -still running
    Queue = []
    last_served = -1
    DC_SIZE = 2048
    FUTURE_TIME = 2000000000
    m = len(TSUC)
    T = [x for x in TSUC if x[0] <= DC_SIZE]
    n = len(T)
    print m, n, n * 1.0 / m



    # intialize all groups and their mappings
    mapping_groups = [group(radix) for i in range((radix/2) +1)]
    mapping_joint = [-1 for i in range(DC_SIZE)]
    mapping_inc = [-1 for i in range(DC_SIZE)]
    mapping_random = [-1 for i in range(DC_SIZE)]
    current_time = 0

    '''
    DC - all the tennant indexs that are still running and take hosts 
    DCmap- an array of all tennats that determines which tennat is still running- (1- still running, 0- finished)
    mapping arrays- all the hosts that are taken or not in the array- a values is a index of a tennat which is holding the host 
    
    '''


    i = 0
    ppp = 0
    while (i < n):
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


        #run till you can make the demnad of tennant i

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


        T[i][I_SCHED] = current_time
        T[i][I_END] = current_time + T[i][I_LEN]



        DC.append(i)
        T[i][I_OTHERS] = len(DC)
        T[i][I_OTHERS_BIG] = len([x for x in DC if T[x][I_SIZE] > 1])
        dc_load = sum([T[j][I_SIZE] for j in DC])


        ######################### our mapping ############################################
        newList=sorted(mapping_groups, key=lambda x: x.avaliableHosts, reverse=False)
        if T[i][I_SIZE] <=  radix/4 :
            for j in range((radix/2)+1):
                if newList[j].avaliableHosts >= T[i][I_SIZE] :
                    pass







        ##################################################################################

        # DC.sort()
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
        for j in range(64):
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

        T[i][I_LEAVES] = [grps_joint, grps_inc, grps_random]
        #        if (i == 3):
        #            print mapping_joint
        T[i][I_SPINES] = [spineformapping(mapping_joint), spineformapping(mapping_inc), spineformapping(mapping_random)]
        if (spineformapping(mapping_joint) > 2) and (ppp == 0):
            print "@", mapping_joint
            ppp = 1
        i = i + 1
    return T


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

        T[i][I_LEAVES] = [grps_joint, grps_inc, grps_random]
        T[i][I_SPINES] = [spineformapping(mapping_joint, DC_SIZE / LEAF_SIZE),
                          spineformapping(mapping_inc, DC_SIZE / LEAF_SIZE),
                          spineformapping(mapping_random, DC_SIZE / LEAF_SIZE)]
        if (spineformapping(mapping_joint) > 2) and (ppp == 0):
            print "@", mapping_joint
            ppp = 1
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
    T = finalschedule(TSUC,16)

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