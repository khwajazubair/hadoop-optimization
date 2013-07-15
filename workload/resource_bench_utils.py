from interact import *
from multiprocessing import Process


def spawn_bench_vms(num_bench,
                    exp_number,
                    placement_map):
    """ Spawns bench VMs according to placement_map """

    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    #
    # Delete all existing instances of cassandra and ycsb
    #
    for i in range(1, num_bench + 1):
        if (BENCH_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[BENCH_HOSTNAME_PREFIX + "-%s" % i].id)

    time.sleep(20)

    sync_glance_index()
    sync_nova_list()

    print "PRINTING", num_bench, placement_map
    for i in range(1, num_bench + 1):
        print "BOOTING"
        nova_boot(IMAGE_MAP["puppet-base"],
                  BENCH_HOSTNAME_PREFIX + "-%s" % i, 4,
                  "--availability-zone=nova:%s" % placement_map[i])

    # Check to see if all VMs have booted
    retries = 10
    success = True

    while (retries != 0):
        print "Retries ", retries
        success = True
        sync_nova_list()
        for i in range(1, num_bench + 1):
            if (not BENCH_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                success = False
                break

        if (success is False):
            print "bench VMs haven't booted yet. \
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


# def run_iperf(num_bench,
#               exp_number,
#               pm):
#     """ Runs iperf between adjacent pairs of servers """
#     BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

#     sync_glance_index()
#     sync_nova_list()

#     # Kill all iperf instances
#     for i in range(1, num_bench + 1):
#         execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i),
#                          "pkill iperf")

#     processlist = []

#     for i in range(1, num_bench + 1):
#         # Spawn server on i'th node
#         execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i),
#                          "iperf -s")

#         time.sleep(4)
#         # Spawn client in (i + 1)'th node with wrap around
#         iperf_client = Process(target=execute_on_vm, args=(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % ((i % num_bench) + 1)),
#                                "iperf -c %s -t 600" % (BENCH_HOSTNAME_PREFIX + "-%s" % i)))
#         iperf_client.start()
#         processlist.append(iperf_client)

#     for process in processlist:
#         process.join()




def run_iperf(num_bench,
              exp_number,
              pm):
    """ Runs iperf between adjacent pairs of servers """
    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    # Kill all iperf instances
    for i in range(1, num_bench + 1):
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i),
                         "pkill iperf")

    processlist = []

    for i in range(1, num_bench + 1, 2):
        # Spawn server on i'th node
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i),
                         "iperf -s")

        time.sleep(4)
        # Spawn client in (i + 1)'th node with wrap around
        for y in range(10):
            iperf_client = Process(target=execute_on_vm, args=(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % ((i % num_bench) + 1)),
                                   "iperf -c %s -t 700" % (BENCH_HOSTNAME_PREFIX + "-%s" % i)))
            iperf_client.start()
            processlist.append(iperf_client)

    for process in processlist:
        process.join()



def load_spew(num_bench,
              exp_number,
              pm):
    """ Create a dummy-file with spew """
    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    # Kill all dd instances
    for i in range(1, num_bench + 1):
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "sudo pkill spew")

    for i in range(1, num_bench + 1):
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "sudo spew %s /mnt/bigfile" % pm["spew:load_params"])


def run_spew(num_bench,
             exp_number,
             pm):
    """ Runs spew on a server """
    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    # Kill all dd instances
    for i in range(1, num_bench + 1):
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "sudo pkill spew")

    for i in range(1, num_bench + 1):
        execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "sudo spew %s  /mnt/bigfile" % pm["spew:run_params"])


def pin_core(physical_machine, instance_name, vcpu, physical_core_list):
    command = "sudo virsh vcpupin %s %s %s" % (get_id_for_instance(instance_name),
                                       vcpu,
                                       reduce(lambda x, y: str(x) + "," + str(y), physical_core_list))
    execute_on_lg(physical_machine, command)


def run_cpu_stress(num_bench,
                   exp_number,
                   pm):
    """ Runs CPU stress on a server """
    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    if (pm["cpu_stress:type"] == "openssl"):
        for i in range(1, num_bench + 1):
            execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "openssl speed -multi 4 && openssl speed -multi 4 && openssl speed -multi 4")
    elif (pm["cpu_stress:type"] == "loop"):
        for i in range(1, num_bench + 1):
            execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "bash cpu-bench.sh loop-bench %s" % pm["cpu_stress:args"])


def end_cpu_stress(num_bench,
                   exp_number,
                   pm):
    """ Kills CPU stress task on a server """
    BENCH_HOSTNAME_PREFIX = "bench-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    if (pm["cpu_stress:type"] == "openssl"):
        for i in range(1, num_bench + 1):
            execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "pkill openssl")
    elif (pm["cpu_stress:type"] == "loop"):
        for i in range(1, num_bench + 1):
            execute_on_vm_bg(get_ip_for_instance(BENCH_HOSTNAME_PREFIX + "-%s" % i), "pkill bash")
