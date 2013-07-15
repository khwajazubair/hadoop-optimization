from interact import *
from placement import *
import time
import sys
from multiprocessing import Process


def spawn_cassandra_vms(num_cassandra,
                        num_ycsb,
                        exp_number,
                        placement_map):
    """ Spawns cassandra and YCSB VMs according to placement_map """

    CASSANDRA_HOSTNAME_PREFIX = "cassandra-4-%s" % exp_number
    YCSB_HOSTNAME_PREFIX = "ycsb-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    #
    # Delete all existing instances of cassandra and ycsb
    #
    for i in range(1, num_cassandra + 1):
        if (CASSANDRA_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[CASSANDRA_HOSTNAME_PREFIX + "-%s" % i].id)

    for i in range(1, num_ycsb + 1):
        if (YCSB_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[YCSB_HOSTNAME_PREFIX + "-%s" % i].id)

    time.sleep(20)

    sync_glance_index()
    sync_nova_list()
    for i in range(1, num_cassandra + 1):
        nova_boot(IMAGE_MAP["cassandra-image"],
                  CASSANDRA_HOSTNAME_PREFIX + "-%s" % i, 4,
                  "--availability-zone=nova:%s" % placement_map[i])

    for i in range(1, num_ycsb + 1):
        nova_boot(IMAGE_MAP["ycsb-image"],
                  YCSB_HOSTNAME_PREFIX + "-%s" % (i), 4, "--availability-zone=nova:%s" % placement_map["ycsb"])

    # Check to see if all VMs have booted
    retries = 10
    success = True

    while (retries != 0):
        print "Retries ", retries
        success = True
        sync_nova_list()
        for i in range(1, num_cassandra + 1):
            if (not CASSANDRA_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                success = False
                break

        if (success is False):
            print "Cassandra VMs haven't booted yet. \
                    Waiting for 10 seconds to recheck"
            retries -= 1
            time.sleep(10)
            continue

        for i in range(1, num_ycsb + 1):
            if (not YCSB_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                success = False
                break

        if (success is False):
            print "YCSB VMs haven't booted yet.\
                    Waiting for 10 seconds to recheck"
            retries -= 1
            time.sleep(10)
            continue
        else:
            break

    if (success is False):
        print "Could not boot all VMs. Exiting"
        print ACTIVE_MAP
        sys.exit(1)
    else:
        print "VMs have booted"


def cassandra_setup(num_cassandra, exp_number, pm):
    """ Start Cassandra processes on VMs, and load the database for ycsb """

    SEED = "cassandra-4-%s-1" % exp_number
    CASSANDRA_HOSTNAME_PREFIX = "cassandra-4-%s" % exp_number
    TIME = time.time()

    sync_glance_index()
    sync_nova_list()

    for i in range(1, num_cassandra + 1):
        scp_file_to_host("application.pp", get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX + "-%s" % i) + ":~/")
        execute_on_vm_bg(get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX + "-%s" % i), "sudo pkill puppet; puppet apply /home/ubuntu/application.pp")
        execute_on_vm_bg(get_ip_for_instance(CASSANDRA_HOSTNAME_PREFIX + "-%s" % i), "cassandra")
        time.sleep(45)  # Need to wait long enough or the bootstrap protocol breaks

    scp_file_to_host(pm["cql"], get_ip_for_instance(SEED) + ":~/")
    execute_on_vm_bg(get_ip_for_instance(SEED), "cassandra-cli -h %s < %s" % (SEED, pm["cql"]))

    # Check if Cassandra nodes have booted successfully

    cassandra_sanity_check(SEED, num_cassandra)

    def f(x):
        if (int(x) < 0):
            return r"\\" + x
        else:
            return x

    # Create and assign tokens to each cassandra node such that
    # they uniformly divide the keyspace

    tokens = map(f, [str(((2**64 / 6) * i) - 2**63) for i in range(num_cassandra)])
    ring = cassandra_get_ring(SEED)  # Returns IPs

    for i in range(1, num_cassandra + 1):
        print ring, i, tokens
        execute_on_vm(ring[i - 1], "nodetool move %s" % (tokens[i - 1]))

    # Check if Cassandra nodes haven't crashed

    if (cassandra_sanity_check(SEED, num_cassandra) == -1):
        print "Cassandra Sanity check failed. Exiting."
        sys.exit(1)
    print "Cassandra load workload terminating after time: " + str(time.time() - TIME)


def cassandra_load_workload(num_cassandra, num_ycsb, exp_number, property_map):
    """ Currently loads the YCSB benchmark """
    SEED = "cassandra-4-%s-1" % exp_number
    TIME = time.time()
    CASSANDRA_HOSTNAME_PREFIX = "cassandra-4-%s" % exp_number
    YCSB_HOSTNAME_PREFIX = "ycsb-%s" % exp_number

    ### YCSB --- load the data
    processlist = []
    for ycsb_id in range(1, num_ycsb + 1):
        props = " -threads 300 -p recordcount=10000000 -p insertstart=%s \
        -p insertcount=2500000 -p measurementtype=timeseries -p timeseries.granularity=10\
         -p cassandra.readconsistencylevel=%s   -p cassandra.writeconsistencylevel=%s -p cassandra.deleteconsistencylevel=%s " \
        % ((2500000/(num_ycsb)) * (ycsb_id - 1), property_map["read_consistency"], property_map["write_consistency"], property_map["delete_consistency"])

        cmd = "cd ycsb-0.1.4; bin/ycsb load cassandra-10 -P workloads/%s -p hosts=" % property_map["cassandra:workload"]
        for i in range(1, num_cassandra + 1):
            cmd = cmd + CASSANDRA_HOSTNAME_PREFIX + "-%s" % i + ","

        cmd = cmd[:-1]
        cmd = cmd + props + " -s &> load.dat"

        print cmd
        ycsbProcess = Process(target=execute_on_vm, args=(get_ip_for_instance(YCSB_HOSTNAME_PREFIX + "-%s" % ycsb_id), cmd))
        processlist.append(ycsbProcess)
        ycsbProcess.start()
        print "LOADING COMPLETE xD"

    cassandra_sanity_check(SEED, num_cassandra)
    for p in processlist:
        p.join()


def cassandra_run_workload(num_cassandra, num_ycsb, exp_number, property_map):
    """ Currently runs the YCSB benchmark """
    SEED = "cassandra-4-%s-1" % exp_number
    TIME = time.time()
    CASSANDRA_HOSTNAME_PREFIX = "cassandra-4-%s" % exp_number
    YCSB_HOSTNAME_PREFIX = "ycsb-%s" % exp_number


    #### YCSB --- run
    processlist = []
    for ycsb_id in range(1, num_ycsb + 1):

        props = " -threads %s -p operationcount=3000000 -p recordcount=10000000 -p insertstart=%s \
        -p insertcount=2500000 -p measurementtype=timeseries -p timeseries.granularity=10 \
        -p cassandra.readconsistencylevel=%s    -p cassandra.writeconsistencylevel=%s  \
         -p cassandra.deleteconsistencylevel=%s" \
        % (property_map["cassandra:threads"], (2500000/(num_ycsb)) * (ycsb_id - 1), property_map["read_consistency"], property_map["write_consistency"], property_map["delete_consistency"])

        cmd = "cd ycsb-0.1.4; bin/ycsb run cassandra-10 -P workloads/%s -p hosts=" % property_map["cassandra:workload"]
        for i in range(1, num_cassandra + 1):
            cmd = cmd + CASSANDRA_HOSTNAME_PREFIX + "-%s" % i + ","

        cmd = cmd[:-1]
        cmd = cmd + props + " -s &> run.dat"

        print cmd
        arg1 = get_ip_for_instance(YCSB_HOSTNAME_PREFIX + "-%s" % ycsb_id)
        ycsbProcess = Process(target=execute_on_vm, args=(arg1, cmd))
        processlist.append(ycsbProcess)
        ycsbProcess.start()

    # This should verify if no nodes crashed in the process
    for p in processlist:
        p.join()

    cassandra_sanity_check(SEED, num_cassandra)
    print "RUN COMPLETE xD"

    print ""

    print "Importing traces"

    for ycsb_id in range(1, num_ycsb + 1):
        expname = "nodeid:%s" % (ycsb_id)
        scp_file_from_host("~/ycsb-0.1.4/load.dat", "runs/load.dat.%s.%s.%s" % (expname, exp_number, TIME), get_ip_for_instance(YCSB_HOSTNAME_PREFIX + "-%s" % ycsb_id))
        scp_file_from_host("~/ycsb-0.1.4/run.dat", "runs/run.dat.%s.%s.%s" % (expname, exp_number, TIME), get_ip_for_instance(YCSB_HOSTNAME_PREFIX + "-%s" % ycsb_id))

    print "Cassandra run workload terminating after time: " + str(time.time() - TIME)



def cassandra_sanity_check(host_to_query, expected):
    ''' Check to see if Cassandra cluster is up and running '''
    retries = 50
    while (retries != 0):
        ret = execute_on_vm(get_ip_for_instance(host_to_query), "nodetool ring")

        total_up = 0
        for row in ret:
            elems = row.split()
            if (len(elems) > 3 and elems[0] != 'Address' and (elems[2] == 'Up' and elems[3] == 'Normal')):
                total_up += 1

        if (total_up != expected):
            print ""
            print "Number of instances was ", total_up, " != NUM_CASSANDRA = ", expected
            print ""
            print "Retrying"
            for row in ret[7:-1]:
                print row
            retries -= 1
            time.sleep(10)
            continue

        for row in ret:
            elems = row.split()
            if (len(elems) > 2 and elems[0] != 'Address' and (elems[2] != 'Up' or elems[3] != 'Normal')):
                print "Cassandra cluster not yet initialised: "
                print ret
                print ""
                print "Retrying"
                print elems
                retries -= 1
                time.sleep(10)
                continue

        print ""
        print "Cassandra cluster is ready!"
        print ""
        break

    if (retries == 0):
        print "Could not start cassandra nodes"
        return -1

    return 0


def cassandra_get_ring(host_to_query):
    ''' Retrieve the Cassandra ring structure '''
    ret = execute_on_vm(get_ip_for_instance(host_to_query), "nodetool ring")
    l = []
    for row in ret:
        elems = row.split()
        if (len(elems) > 2 and elems[0] != 'Address'and (elems[1] == 'rack1')):
            l.append(elems[0])
    # return map(lambda x: x.split()[0], ret[7:-1])
    return l