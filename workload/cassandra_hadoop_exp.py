from interact import *
from placement import *
from random import shuffle
import time
import sys
from multiprocessing import Process

NUM_CASSANDRA = 6
NUM_YCSB = 2
NODE_LIST = ["loadgen162",
             "loadgen163",
             "loadgen164",
             "loadgen165",
             "loadgen166",
             "loadgen167"]


def run_cassandra_hadoop_experiment(num_cassandra,
                                     num_ycsb,
                                     exp_number,
                                     threads,
                                     workload,
                                     placement_map,
                                     experimentid):
    TIME = time.time()
    CASSANDRA_HOSTNAME_PREFIX = "cassandra-4-%s" % exp_number
    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
    SEED = "cassandra-4-%s-1" % exp_number
    num_hadoop = len(placement_map)

    sync_glance_index()
    sync_nova_list()

    for i in range(1, num_cassandra + 1):
        if (CASSANDRA_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[CASSANDRA_HOSTNAME_PREFIX + "-%s" % i].id)

    for i in range(1, num_ycsb + 1):
        if ("ycsb-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP["ycsb-%s" % i].id)

    for i in range(1, num_hadoop + 1):
        if (HADOOP_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[HADOOP_HOSTNAME_PREFIX + "-%s" % i].id)

    time.sleep(20)

    sync_glance_index()
    sync_nova_list()
    for i in range(1, num_cassandra + 1):
        nova_boot(IMAGE_MAP["cassandra-image"],
                  CASSANDRA_HOSTNAME_PREFIX + "-%s" % i, 4,
                  "--availability-zone=nova:%s" % placement_map[i])

    for i in range(1, num_ycsb + 1):
        nova_boot(IMAGE_MAP["ycsb-image"],
                  "ycsb-%s" % (i), 5, "--availability-zone=nova:loadgen171")

    nova_boot(IMAGE_MAP["hadoop-swim-nn"],
              HADOOP_HOSTNAME_PREFIX + "-1", 4,
              "--availability-zone=nova:%s" % placement_map[1])

    nova_boot(IMAGE_MAP["hadoop-dn"],
              HADOOP_HOSTNAME_PREFIX + "-2", 4,
              "--availability-zone=nova:%s" % placement_map[3])

    nova_boot(IMAGE_MAP["hadoop-dn"],
              HADOOP_HOSTNAME_PREFIX + "-3 ", 4,
              "--availability-zone=nova:%s" % placement_map[5])


    # ###### Sanity check to see if all VMs have booted
    # retries = 10
    # success = True

    # while (retries != 0):
    #     print "Retries ", retries
    #     success = True
    #     sync_nova_list()
    #     for i in range(1, num_cassandra + 1):
    #         if (not CASSANDRA_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP.has_key):
    #             success = False
    #             break

    #     if (success is False):
    #         print "Cassandra VMs haven't booted yet. \
    #                 Waiting for 10 seconds to recheck"
    #         retries -= 1
    #         time.sleep(10)
    #         continue

    #     for i in range(1, num_ycsb + 1):
    #         if (not "ycsb-%s" % i in ACTIVE_MAP.has_key):
    #             success = False
    #             break

    #     if (success is False):
    #         print "YCSB VMs haven't booted yet.\
    #                 Waiting for 10 seconds to recheck"
    #         retries -= 1
    #         time.sleep(10)
    #         continue
    #     else:
    #         break

    # if (success is False):
    #     print "Could not boot all VMs. Exiting"
    #     print ACTIVE_MAP
    #     sys.exit(-1)
    # else:
    #     print "VMs have booted"

    # # Leave some time for the nodes to fully boot
    # time.sleep(80)

    # ####### Start cassandra on the VMs

    # for i in range(1, num_cassandra + 1):
    #     sync_glance_index()
    #     sync_nova_list()
    #     scp_file_to_host("application.pp",
    #                      get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX
    #                                          + "-%s" % i) + ":~/")
    #     execute_on_vm_bg(get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX + "-%s" % i), "sudo pkill puppet; puppet apply /home/ubuntu/application.pp")
    #     execute_on_vm_bg(get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX + "-%s" % i), "cassandra")
    #     time.sleep(45)

    # scp_file_to_host("cassandra.cql", get_ip_for_instance("cassandra-4-4-1") + ":~/")
    # execute_on_vm_bg(get_ip_for_instance(SEED), "cassandra-cli -h cassandra-4-4-1 < cassandra.cql")

    # # Check if it went well

    # cassandra_sanity_check(SEED, num_cassandra)

    # def f(x):
    #     if (int(x) < 0):
    #         return r"\\" + x
    #     else:
    #         return x

    # tokens = map(f, [str(((2**64 / 6) * i) - 2**63) for i in range(num_cassandra)])
    # ring = cassandra_get_ring(SEED) # Returns IPs
    # for i in range(1, num_cassandra + 1):
    #     print ring[i - 1], tokens[i - 1]
    #     execute_on_vm(ring[i - 1], "nodetool move %s" % (tokens[i - 1]))

    # cassandra_sanity_check(SEED, num_cassandra)
    # # #### This is where the workload generation happens

    # #### YCSB --- load the data
    # processlist = []
    # for ycsb_id in range(1, num_ycsb + 1):
    #     props = " -threads %s -p recordcount=1000000 -p insertstart=%s \
    #     -p insertcount=250000 -p measurementtype=timeseries -p timeseries.granularity=10 " \
    #     % (threads, (250000/(num_ycsb)) * (ycsb_id - 1))

    #     cmd = "cd ycsb-0.1.4; bin/ycsb load cassandra-10 -P workloads/%s -p hosts=" % workload
    #     for i in range(1, num_cassandra + 1):
    #         cmd = cmd + CASSANDRA_HOSTNAME_PREFIX + "-%s" % i + ","

    #     cmd = cmd[:-1]
    #     cmd = cmd + props + " -s &> load.dat"

    #     print cmd
    #     ycsbProcess = Process(target=execute_on_vm, args=(get_ip_for_instance("ycsb-%s" % ycsb_id), cmd))
    #     processlist.append(ycsbProcess)
    #     ycsbProcess.start()
    #     print "LOADING COMPLETE xD"

    # cassandra_sanity_check(SEED, num_cassandra)
    # for p in processlist:
    #     p.join()

    #  # execute_on_vm(get_ip_for_instance(SEED), "sudo tc qdisc add dev eth0 root netem delay 200ms")
    # time.sleep(30)

    # #### YCSB --- run
    # processlist = []
    # for ycsb_id in range(1, num_ycsb + 1):

    #     props = " -threads %s -p operationcount=1000000 -p recordcount=1000000 -p insertstart=%s \
    #     -p insertcount=250000 -p measurementtype=timeseries -p timeseries.granularity=10 \
    #     -p cassandra.readconsistencylevel=QUORUM -p cassandra.writeconsistencylevel=QUORUM \
    #      -p cassandra.deleteconsistencylevel=QUORUM" \
    #     % (threads, (250000/(num_ycsb)) * (ycsb_id - 1))

    #     cmd = "cd ycsb-0.1.4; bin/ycsb run cassandra-10 -P workloads/%s -p hosts=" % workload
    #     for i in range(1, num_cassandra + 1):
    #         cmd = cmd + CASSANDRA_HOSTNAME_PREFIX + "-%s" % i + ","

    #     cmd = cmd[:-1]
    #     cmd = cmd + props + " -s &> run.dat"

    #     print cmd
    #     arg1 = get_ip_for_instance("ycsb-%s" % ycsb_id)
    #     ycsbProcess = Process(target=execute_on_vm, args=(arg1, cmd))
    #     processlist.append(ycsbProcess)
    #     ycsbProcess.start()

    # # This should verify if no nodes crashed in the process
    # for p in processlist:
    #     p.join()

    # cassandra_sanity_check(SEED, num_cassandra)
    # print "RUN COMPLETE xD"

    # print ""

    # print "Importing traces"

    # for ycsb_id in range(1, num_ycsb + 1):
    #     expname = "strategy:%s-nodeid:%s-numcassandra:%s-numycsb:%s-expnum:%s-threads:%s-operationcount:1000-workload:%s" % (experimentid, ycsb_id, num_cassandra, num_ycsb, exp_number, threads, workload)
    #     scp_file_from_host("~/ycsb-0.1.4/load.dat", "runs/load.dat.%s.%s" % (expname, TIME), get_ip_for_instance("ycsb-%s" % ycsb_id))
    #     scp_file_from_host("~/ycsb-0.1.4/run.dat", "runs/run.dat.%s.%s" % (expname, TIME), get_ip_for_instance("ycsb-%s" % ycsb_id))


for run_no in range(1, 2):
    for n_threads in [200]:
        for workload in ["workloada"]: #, "workloadb", "workloadc", "workloadd", "workloade", "workloadf"]:
            print n_threads
            shuffle(NODE_LIST)
            run_cassandra_hadoop_experiment(NUM_CASSANDRA, NUM_YCSB,
                                     4, n_threads, workload, STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA), "OneToOne")