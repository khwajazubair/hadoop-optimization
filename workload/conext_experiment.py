from interact import *
from placement import *
from cassandra_utils import *
from hadoop_utils import *
from resource_bench_utils import *
import simplejson
import time
import glob
import os
import shutil
from common_monitors import *
from multiprocessing import Process

NUM_CASSANDRA = 6
NUM_YCSB = 1
NODE_LIST = ["loadgen162", "loadgen163", "loadgen164", "loadgen165", "loadgen166", "loadgen167"]


def run_cassandra_baseline(pm, nodes_used):
    ''' This is a single run to gather the baseline resource
        and performance profile for Cassandra  '''

    ###### SPAWN VMS ##########
    spawn_cassandra_vms(pm["cassandra:num_cassandra"],
                        pm["cassandra:num_ycsb"],
                        pm["exp_number"],
                        pm["cassandra:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    cassandraStartProcess = Process(target=cassandra_setup, args=(pm["cassandra:num_cassandra"], pm["exp_number"]))
    cassandraStartProcess.start()
    cassandraStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    cassandraLoadProcess = Process(target=cassandra_load_workload,
                                   args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    cassandraLoadProcess.start()
    cassandraLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_bwmon(nodes_used, pm["cassandra:experimentid"])
    run_iostat(nodes_used, pm["cassandra:experimentid"])
    run_mpstat(nodes_used, pm["cassandra:experimentid"])

    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    time.sleep(40)
    cassandraRunProcess.start()
    cassandraRunProcess.join()
    time.sleep(10)
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])
    retreive_mpstat_results(nodes_used, pm["cassandra:experimentid"])


def cassandra_baseline_experiment(exp_number):
    ''' Defines the experiment to run the Cassandra baseline'''

    pm = {}
    pm["exp_number"] = exp_number
    pm["cassandra:num_cassandra"] = NUM_CASSANDRA
    pm["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                files = glob.glob('runs/*')
                for f in files:
                    os.remove(f)

                pm["cassandra:threads"] = n_threads
                pm["cassandra:workload"] = workload

                ################# One-to-One #############
                pm["cassandra:experimentid"] = "OneToOne"
                pm["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                run_cassandra_baseline(pm, NODE_LIST + ["loadgen171"])

                ### Dump the property map to a config file
                config_dump = open('runs/conf.exp', 'w')
                config_dump.write(simplejson.dumps(pm, indent=4))
                config_dump.close()

                shutil.copytree("runs", "runs-%s-%s-%s" % ("cassandra-baseline-experiment", exp_number, int(time.time())))

                files = glob.glob('runs/*')

                for f in files:
                    os.remove(f)


def run_cassandra_spew(pm, nodes_used):

    ###### SPAWN VMS ##########
    spawn_cassandra_vms(pm["cassandra:num_cassandra"],
                        pm["cassandra:num_ycsb"],
                        pm["exp_number"],
                        pm["cassandra:placement"])
    spawn_bench_vms(pm["spew:num_spew"],
                    pm["exp_number"],
                    pm["spew:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    cassandraStartProcess = Process(target=cassandra_setup, args=(pm["cassandra:num_cassandra"], pm["exp_number"]))
    cassandraStartProcess.start()
    cassandraStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    cassandraLoadProcess = Process(target=cassandra_load_workload,
                                   args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    spewLoadProcess = Process(target=load_spew,
                            args=(pm["spew:num_spew"], pm["exp_number"], pm))
    cassandraLoadProcess.start()
    spewLoadProcess.start()
    cassandraLoadProcess.join()
    spewLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    run_bwmon(nodes_used, pm["cassandra:experimentid"])
    run_iostat(nodes_used, pm["cassandra:experimentid"])
    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    spew_process = Process(target=run_spew, args=(pm["spew:num_spew"],
                           pm["exp_number"],
                           pm))
    spew_process.start()
    time.sleep(40)
    cassandraRunProcess.start()
    cassandraRunProcess.join()
    spew_process.join()
    time.sleep(10)
    retreive_mpstat_results(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])


def spew_cassandra_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number
    pm["cassandra:num_cassandra"] = NUM_CASSANDRA
    pm["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                for spew_run_params in ["--write -b 256k 80g",
                                        "--write -r -b 256k 85g",
                                        "--write -b 16k 80g",
                                        "--write -r -b 16k 80g",]:
                    # for stress_node in NODE_LIST + ["loadgen171"] + [NODE_LIST[:len(NODE_LIST)/2]] + [NODE_LIST]:
                    for stress_node in [NODE_LIST]:
                        files = glob.glob('runs/*')
                        for f in files:
                            os.remove(f)

                        pm["spew:load_params"] = "--help"
                        pm["spew:run_params"] = spew_run_params
                        pm["cassandra:threads"] = n_threads
                        pm["cassandra:workload"] = workload

                        ################# One-to-One #############
                        pm["cassandra:experimentid"] = "OneToOne"
                        pm["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)

                        if (isinstance(stress_node, list)):
                            pm["spew:num_spew"] = len(stress_node)
                            pm["spew:placement"] = STRATEGIES["one-to-one"](stress_node, pm["spew:num_spew"])
                        elif (isinstance(stress_node, basestring)):
                            pm["spew:num_spew"] = 1
                            pm["spew:placement"] = STRATEGIES["one-to-one"]([stress_node], pm["spew:num_spew"])

                        run_cassandra_spew(pm, NODE_LIST)

                        ### Dump the property map to a config file
                        config_dump = open('runs/conf.exp', 'w')
                        config_dump.write(simplejson.dumps(pm, indent=4))
                        config_dump.close()

                        shutil.copytree("runs", "runs-%s-%s-%s" % ("spew-read-cassandra-experiment", exp_number, int(time.time())))

                        files = glob.glob('runs/*')

                        for f in files:
                            os.remove(f)


def run_cassandra_cpu(pm, nodes_used):

    ###### SPAWN VMS ##########
    spawn_cassandra_vms(pm["cassandra:num_cassandra"],
                        pm["cassandra:num_ycsb"],
                        pm["exp_number"],
                        pm["cassandra:placement"])
    spawn_bench_vms(pm["cpu_stress:num_cpu"],
                    pm["exp_number"],
                    pm["cpu_stress:placement"])
    time.sleep(60)

    ####### START PROCESSES ##########

    cassandraStartProcess = Process(target=cassandra_setup, args=(pm["cassandra:num_cassandra"], pm["exp_number"]))
    cassandraStartProcess.start()
    cassandraStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    cassandraLoadProcess = Process(target=cassandra_load_workload,
                                   args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    cassandraLoadProcess.start()
    cassandraLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    for vm_id in pm["cassandra:placement"]:
        for vcpu in range(0,4): # Assumes only 4 vcpus
            pin_core(pm["cassandra:placement"][vm_id],
                     "cassandra-4-%s-%s" % (pm["exp_number"], vm_id),
                     vcpu,
                     [vcpu + 4])

    for vcpu in range(0,4): # Assumes only 4 vcpus
        pin_core("loadgen171",
                 "ycsb-1",
                 vcpu,
                 [vcpu + 4])

    for vm_id in pm["cpu_stress:placement"]:
        for vcpu in range(0,4): # Assumes only 4 vcpus
            pin_core(pm["cpu_stress:placement"][vm_id],
                     "bench-%s-%s" % (pm["exp_number"], vm_id),
                     vcpu,
                     [vcpu + 4])

    run_mpstat(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    run_bwmon(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    run_iostat(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])

    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    cpu_process = Process(target=run_cpu_stress, args=(pm["cpu_stress:num_cpu"],
                           pm["exp_number"],
                           pm))
    cpu_process.start()
    time.sleep(40)
    cassandraRunProcess.start()

    cassandraRunProcess.join()

    end_cpu_stress(pm["cpu_stress:num_cpu"],
                   pm["exp_number"],
                   pm)
    cpu_process.join()

    time.sleep(10)

    retreive_mpstat_results(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    retreive_bwmon_results(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used + ["loadgen171"], pm["cassandra:experimentid"])


def cpu_cassandra_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number
    pm["cassandra:num_cassandra"] = NUM_CASSANDRA
    pm["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                for spew_run_params in ["openssl"]:
                    # for stress_node in NODE_LIST + ["loadgen171"] + [NODE_LIST[:len(NODE_LIST)/2]] + [NODE_LIST]:
                    for stress_node in [NODE_LIST[:len(NODE_LIST)/2]] + [NODE_LIST]:
                        files = glob.glob('runs/*')
                        for f in files:
                            os.remove(f)

                        pm["cpu_stress:type"] = spew_run_params
                        pm["cassandra:threads"] = n_threads
                        pm["cassandra:workload"] = workload

                        ################# One-to-One #############
                        pm["cassandra:experimentid"] = "OneToOne"
                        pm["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)

                        if (isinstance(stress_node, list)):
                            pm["cpu_stress:num_cpu"] = len(stress_node)
                            pm["cpu_stress:placement"] = STRATEGIES["one-to-one"](stress_node, pm["cpu_stress:num_cpu"])
                        elif (isinstance(stress_node, basestring)):
                            pm["cpu_stress:num_cpu"] = 1
                            pm["cpu_stress:placement"] = STRATEGIES["one-to-one"]([stress_node], pm["cpu_stress:num_cpu"])

                        run_cassandra_cpu(pm, NODE_LIST + ["loadgen171"])

                        ### Dump the property map to a config file
                        config_dump = open('runs/conf.exp', 'w')
                        config_dump.write(simplejson.dumps(pm, indent=4))
                        config_dump.close()

                        shutil.copytree("runs", "runs-%s-%s-%s" % ("cpu-read-cassandra-experiment", exp_number, int(time.time())))

                        files = glob.glob('runs/*')

                        for f in files:
                            os.remove(f)


def run_cassandra_iperf(pm, nodes_used):

    ####### SPAWN VMS ##########
    spawn_cassandra_vms(pm["cassandra:num_cassandra"],
                        pm["cassandra:num_ycsb"],
                        pm["exp_number"],
                        pm["cassandra:placement"])
    spawn_bench_vms(pm["iperf:num_iperf"],
                    pm["exp_number"],
                    pm["iperf:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    cassandraStartProcess = Process(target=cassandra_setup, args=(pm["cassandra:num_cassandra"], pm["exp_number"], pm))
    cassandraStartProcess.start()
    cassandraStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    cassandraLoadProcess = Process(target=cassandra_load_workload,
                                   args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    cassandraLoadProcess.start()
    cassandraLoadProcess.join()

    time.sleep(30)

    ###### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["cassandra:experimentid"])
    run_bwmon(nodes_used, pm["cassandra:experimentid"])
    run_iostat(nodes_used, pm["cassandra:experimentid"])
    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    iperf_process = Process(target=run_iperf, args=(pm["iperf:num_iperf"],
                            pm["exp_number"],
                            pm))
    iperf_process.start()
    time.sleep(40)
    cassandraRunProcess.start()
    cassandraRunProcess.join()
    iperf_process.join()
    time.sleep(10)
    retreive_mpstat_results(nodes_used, pm["cassandra:experimentid"])
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])


def iperf_cassandra_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number
    pm["cassandra:num_cassandra"] = NUM_CASSANDRA
    pm["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                # for stress_node in NODE_LIST + ["loadgen171"]:
                # for stress_node in NODE_LIST + ["loadgen171"]:
                for stress_node in ["loadgen162", "loadgen163", "loadgen165", "loadgen166", "loadgen167", "loadgen171"]:
                    # stress_node = ["loadgen162", "loadgen168",
                    #                "loadgen165", "loadgen169",
                    #                "loadgen167", "loadgen170"]
                    # stress_node = ["loadgen171", "loadgen169"]

                    files = glob.glob('runs/*')
                    for f in files:
                       os.remove(f)

                    pm["cassandra:threads"] = n_threads
                    pm["cassandra:workload"] = workload

                    ################# One-to-One #############
                    pm["cassandra:experimentid"] = "OneToOne"
                    pm["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                    pm["iperf:num_iperf"] = 2 # CHANGE THIS!!!!!
                    pm["iperf:placement"] = STRATEGIES["round-robin"]([stress_node, "loadgen169"], 2)

                    pm["read_consistency"] = "ONE"
                    pm["write_consistency"] = "ONE"
                    pm["delete_consistency"] = "ONE"
                    pm["cql"] = "cassandra_no_repl.cql"
                    pm["cassandra:placement"]["ycsb"] = "loadgen171"
                    run_cassandra_iperf(pm, NODE_LIST + ["loadgen171"])

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("iperf-model-cassandra-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)


def run_hadoop_baseline(pm, nodes_used):
    ''' This is a single run to gather the baseline resource
        and performance profile for Cassandra  '''

    ###### SPAWN VMS ##########
    spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
    hadoopStartProcess.start()
    hadoopStartProcess.join()
    time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm,))
    hadoopLoadProcess.start()
    hadoopLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])
    hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm,))
    time.sleep(40)

    hadoopRunProcess.start()
    hadoopRunProcess.join()
    time.sleep(180)
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def baseline_hadoop_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number

    for run_no in range(1, 2):
        for n_threads in [50]:
            files = glob.glob('runs/*')
            for f in files:
                os.remove(f)
            ################# One-to-One #############
            pm["hadoop:num_hadoop"] = len(NODE_LIST)
            pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))

            run_hadoop_baseline(pm, NODE_LIST)

            ### Dump the property map to a config file
            config_dump = open('runs/conf.exp', 'w')
            config_dump.write(simplejson.dumps(pm, indent=4))
            config_dump.close()

            shutil.copytree("runs", "runs-%s-%s-%s" % ("baseline-hadoop-experiment", exp_number, int(time.time())))

            files = glob.glob('runs/*')

            for f in files:
                os.remove(f)


def run_hadoop_spew(pm, nodes_used):

    ###### SPAWN VMS ##########
    spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])

    spawn_bench_vms(pm["spew:num_spew"],
                    pm["exp_number"],
                    pm["spew:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
    hadoopStartProcess.start()
    hadoopStartProcess.join()
    time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm,))
    spewLoadProcess = Process(target=load_spew,
                            args=(pm["spew:num_spew"], pm["exp_number"], pm))
    hadoopLoadProcess.start()
    spewLoadProcess.start()
    hadoopLoadProcess.join()
    spewLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])
    hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm,))

    spew_process = Process(target=run_spew, args=(pm["spew:num_spew"],
                           pm["exp_number"],
                           pm))
    spew_process.start()
    time.sleep(40)
    hadoopRunProcess.start()
    hadoopRunProcess.join()
    time.sleep(10)
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def spew_hadoop_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number

    for run_no in range(1, 2):
        for n_threads in [50]:
            for spew_run_params in ["--write -b 256k 180g",
                        "--write -r -b 256k 180g",
                        "--write -b 16k 180g",
                        "--write -r -b 16k 180g",]:
                for stress_node in NODE_LIST + [NODE_LIST[len(NODE_LIST)/2:]] + [NODE_LIST[1:]]:
                    files = glob.glob('runs/*')
                    for f in files:
                        os.remove(f)

                    pm["spew:load_params"] = "--help"
                    pm["spew:run_params"] = spew_run_params

                    ################# One-to-One #############
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))

                    if (isinstance(stress_node, list)):
                        pm["spew:num_spew"] = len(stress_node)
                        pm["spew:placement"] = STRATEGIES["one-to-one"](stress_node, pm["spew:num_spew"])
                    elif (isinstance(stress_node, basestring)):
                        pm["spew:num_spew"] = 1
                        pm["spew:placement"] = STRATEGIES["one-to-one"]([stress_node], pm["spew:num_spew"])

                    run_hadoop_spew(pm, NODE_LIST)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("spew-read-hadoop-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)


def run_hadoop_cpu(pm, nodes_used):

    ###### SPAWN VMS ##########
    spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])

    spawn_bench_vms(pm["cpu_stress:num_cpu"],
                    pm["exp_number"],
                    pm["cpu_stress:placement"])
    time.sleep(60)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
    hadoopStartProcess.start()
    hadoopStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm,))
    hadoopLoadProcess.start()
    hadoopLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    for vm_id in pm["hadoop:placement"]:
        for vcpu in range(0,4): # Assumes only 4 vcpus
            pin_core(pm["hadoop:placement"][vm_id],
                     "hadoop-%s-%s" % (pm["exp_number"], vm_id),
                     vcpu,
                     [vcpu + 4])

    for vm_id in pm["cpu_stress:placement"]:
        for vcpu in range(0,4): # Assumes only 4 vcpus
            pin_core(pm["cpu_stress:placement"][vm_id],
                     "bench-%s-%s" % (pm["exp_number"], vm_id),
                     vcpu,
                     [vcpu + 4])

    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])

    hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm,))
    cpu_process = Process(target=run_cpu_stress, args=(pm["cpu_stress:num_cpu"],
                          pm["exp_number"],
                          pm))
    cpu_process.start()
    time.sleep(40)
    hadoopRunProcess.start()

    hadoopRunProcess.join()

    end_cpu_stress(pm["cpu_stress:num_cpu"],
                   pm["exp_number"],
                   pm)
    cpu_process.join()

    time.sleep(10)

    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def cpu_hadoop_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number

    for run_no in range(1, 2):
        for n_threads in [50]:
            for cpu_stress_params in ["openssl"]:
                for stress_node in [NODE_LIST[len(NODE_LIST)/2:]] + [NODE_LIST[1:]]:
                # for stress_node in NODE_LIST + [NODE_LIST[len(NODE_LIST)/2:]] + [NODE_LIST[1:]]:
                    files = glob.glob('runs/*')
                    for f in files:
                        os.remove(f)

                    pm["cpu_stress:type"] = cpu_stress_params

                    ################# One-to-One #############
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))

                    if (isinstance(stress_node, list)):
                        pm["cpu_stress:num_cpu"] = len(stress_node)
                        pm["cpu_stress:placement"] = STRATEGIES["one-to-one"](stress_node, pm["cpu_stress:num_cpu"])
                    elif (isinstance(stress_node, basestring)):
                        pm["cpu_stress:num_cpu"] = 1
                        pm["cpu_stress:placement"] = STRATEGIES["one-to-one"]([stress_node], pm["cpu_stress:num_cpu"])

                    run_hadoop_cpu(pm, NODE_LIST)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("cpu-read-hadoop-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)


def run_hadoop_iperf(pm, nodes_used):


    ###### SPAWN VMS ##########
    spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])

    spawn_bench_vms(pm["iperf:num_iperf"],
                    pm["exp_number"],
                    pm["iperf:placement"])
    time.sleep(60)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
    hadoopStartProcess.start()
    hadoopStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm,))
    hadoopLoadProcess.start()
    hadoopLoadProcess.join()
    time.sleep(30)

    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])

    hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm,))
    iperf_process = Process(target=run_iperf, args=(pm["iperf:num_iperf"],
                            pm["exp_number"],
                            pm))
    iperf_process.start()
    time.sleep(40)
    hadoopRunProcess.start()

    hadoopRunProcess.join()

    iperf_process.join()

    time.sleep(10)

    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def iperf_hadoop_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                # for stress_node in NODE_LIST + [NODE_LIST[len(NODE_LIST)/2:]] + [NODE_LIST[1:]]:
                    stress_node = ["loadgen163", "loadgen169",
                                   "loadgen164", "loadgen170",
                                   "loadgen165", "loadgen171",]

                    files = glob.glob('runs/*')
                    for f in files:
                       os.remove(f)

                    ################# One-to-One #############
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                    pm["iperf:num_iperf"] = len(stress_node)
                    pm["iperf:placement"] = STRATEGIES["round-robin"](stress_node, len(stress_node))
                    run_hadoop_iperf(pm, NODE_LIST)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("iperf-read-hadoop-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)




def evaluation_experiment(exp_number, num_cstar, num_cnonstar, num_hadoop, PLIST):
    ''' Defines the experiment to run the Cassandra baseline'''

    nodes_used = NODE_LIST + ["loadgen171"]
    pm = {}
    pm["exp_number_cassandra_star"] = 35
    pm["exp_number_cassandra_nostar"] = 40
    pm["exp_number_hadoop"] = 45

    files = glob.glob('runs/*')
    for f in files:
        os.remove(f)

    pm["cassandra:threads"] = 40
    pm["cassandra:workload"] = "workload_read_only"
    pm["cassandra:star:num_cassandra"] = 5
    pm["cassandra:nostar:num_cassandra"] = 4
    pm["hadoop:num_hadoop"] = 5

    ################# One-to-One #############
    pm["cassandra:experimentid"] = "OneToOne"
    pm["plist"] = PLIST

    ############ Good case ##############
    # PLIST = []
    # pm["cassandra:nostar:placement"] = {1 : "loadgen171",
    #                                     2 : "loadgen164",
    #                                     3 : "loadgen165",
    #                                     4 : "loadgen166",
    #                                     "ycsb" : "loadgen167"}

    # pm["cassandra:star:placement"] = {1 : "loadgen162",
    #                                   2 : "loadgen171",
    #                                   3 : "loadgen164",
    #                                   4 : "loadgen165",
    #                                   5 : "loadgen166",
    #                                   "ycsb" : "loadgen163"}

    # pm["hadoop:placement"] = {1 : "loadgen162",
    #                           2 : "loadgen163",
    #                           3 : "loadgen164",
    #                           4 : "loadgen165",
    #                           5 : "loadgen166",
    #                           }

    ############ Interference inverted ##############
    # pm["cassandra:nostar:placement"] = {"ycsb": 'loadgen167',
    #                                       1: 'loadgen171',
    #                                       2: 'loadgen164',
    #                                       3: 'loadgen165',
    #                                       4: 'loadgen166'}

    # pm["cassandra:star:placement"] = {"ycsb": 'loadgen171', 1: 'loadgen163', 2: 'loadgen167', 3: 'loadgen162', 4: 'loadgen164', 5: 'loadgen165'}

    # pm["hadoop:placement"] = {1: 'loadgen162', 2: 'loadgen163', 3: 'loadgen164', 4: 'loadgen165', 5: 'loadgen166'}

    ############ Priority and interference inverted ##############
    # pm["cassandra:nostar:placement"] = {"ycsb": 'loadgen167', 1: 'loadgen171', 2: 'loadgen167', 3: 'loadgen167', 4: 'loadgen167'}
    # pm["cassandra:star:placement"] = {"ycsb": 'loadgen167', 1: 'loadgen162', 2: 'loadgen167', 3: 'loadgen167', 4: 'loadgen167', 5: 'loadgen167'}
    # pm["hadoop:placement"] = {1: 'loadgen162', 2: 'loadgen163', 3: 'loadgen164', 4: 'loadgen165', 5: 'loadgen166'}

    ############# Priority only inverted ##############
    # pm["cassandra:nostar:placement"] = {"ycsb": 'loadgen167', 1: 'loadgen171', 2: 'loadgen162', 3: 'loadgen167', 4: 'loadgen167'}
    # pm["cassandra:star:placement"] = {"ycsb": 'loadgen167', 1: 'loadgen163', 2: 'loadgen164', 3: 'loadgen165', 4: 'loadgen166', 5: 'loadgen167'}
    # pm["hadoop:placement"] = {1: 'loadgen162', 2: 'loadgen163', 3: 'loadgen164', 4: 'loadgen165', 5: 'loadgen166'}
    ###########

    ###### SPAWN VMS ##########
    #star
    for i in range(num_cstar):
        spawn_cassandra_vms(pm["cassandra:star:num_cassandra"],
                            1,
                            pm["exp_number_cassandra_star"] + i,
                            PLIST[0 + i])

    #nostar
    for i in range(num_cnonstar):
        spawn_cassandra_vms(pm["cassandra:nostar:num_cassandra"],
                        1,
                        pm["exp_number_cassandra_nostar"] + i,
                        PLIST[1 + i])

    #hadoop
    for i in range(num_hadoop):
        spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                         pm["exp_number_hadoop"] + i,
                         PLIST[2 + i])

    time.sleep(120)

    ####### START PROCESSES ##########

    processList = []
    for i in range(num_cstar):
        pm["cql"] = "cassandra.cql"
        cassandraStartProcess_star = Process(target=cassandra_setup, args=(pm["cassandra:star:num_cassandra"], pm["exp_number_cassandra_star"] + i, pm))
        cassandraStartProcess_star.start()
        processList.append(cassandraStartProcess_star)


    for i in range(num_cnonstar):
        pm["cql"] = "cassandra_no_repl.cql"
        cassandraStartProcess_nonstar = Process(target=cassandra_setup, args=(pm["cassandra:nostar:num_cassandra"], pm["exp_number_cassandra_nostar"] + i, pm))
        cassandraStartProcess_nonstar.start()
        processList.append(cassandraStartProcess_nonstar)

    for i in range(num_hadoop):
        hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number_hadoop"] + i))
        hadoopStartProcess.start()
        processList.append(hadoopStartProcess)

    for process in processList:
        process.join()

    time.sleep(120)

    ####### LOAD DATA ##########
    processList = []
    for i in range(num_cnonstar):
        pm["read_consistency"] = "ONE"
        pm["write_consistency"] = "ONE"
        pm["delete_consistency"] = "ONE"
        cassandraLoadProcess_nostar = Process(target=cassandra_load_workload,
                                       args=(pm["cassandra:nostar:num_cassandra"], 1, pm["exp_number_cassandra_nostar"] + i, pm))
        cassandraLoadProcess_nostar.start()
        processList.append(cassandraLoadProcess_nostar)

    for i in range(num_cstar):
        pm["read_consistency"] = "ONE"
        pm["write_consistency"] = "QUORUM"
        pm["delete_consistency"] = "QUORUM"
        cassandraLoadProcess_star = Process(target=cassandra_load_workload,
                                       args=(pm["cassandra:star:num_cassandra"], 1, pm["exp_number_cassandra_star"] + i, pm))
        cassandraLoadProcess_star.start()
        processList.append(cassandraLoadProcess_star)

    for i in range(num_hadoop):
        hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm, pm["exp_number_hadoop"] + i))
        hadoopLoadProcess.start()
        processList.append(hadoopLoadProcess)

    for process in processList:
        process.join()

    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_bwmon(nodes_used, pm["cassandra:experimentid"])
    run_iostat(nodes_used, pm["cassandra:experimentid"])
    run_mpstat(nodes_used, pm["cassandra:experimentid"])

    processList = []
    for i in range(num_cnonstar):

        pm["read_consistency"] = "ONE"
        pm["write_consistency"] = "ONE"
        pm["delete_consistency"] = "ONE"
        cassandraRunProcess_nostar = Process(target=cassandra_run_workload,
                                      args=(pm["cassandra:nostar:num_cassandra"], 1, pm["exp_number_cassandra_nostar"] + i, pm))
        cassandraRunProcess_nostar.start()
        processList.append(cassandraRunProcess_nostar)


    for i in range(num_cstar):
        pm["read_consistency"] = "ONE"
        pm["write_consistency"] = "QUORUM"
        pm["delete_consistency"] = "QUORUM"
        cassandraRunProcess_star = Process(target=cassandra_run_workload,
                                      args=(pm["cassandra:star:num_cassandra"], 1, pm["exp_number_cassandra_star"] + i, pm))
        cassandraRunProcess_star.start()
        processList.append(cassandraRunProcess_star)

    for i in range(num_hadoop):
        hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm, pm["exp_number_hadoop"] + i))
        hadoopRunProcess.start()
        processList.append(hadoopRunProcess)


    for process in processList:
        process.join()

    time.sleep(30)


    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])
    retreive_mpstat_results(nodes_used, pm["cassandra:experimentid"])



    ### Dump the property map to a config file
    config_dump = open('runs/conf.exp', 'w')
    config_dump.write(simplejson.dumps(pm, indent=4))
    config_dump.close()

    shutil.copytree("runs", "runs-%s-%s-%s" % ("macromap-macromap-experiment", exp_number, int(time.time())))

    files = glob.glob('runs/*')

    for f in files:
        os.remove(f)


def prepare_random_placement_list(num_cstar, num_cnonstar, num_hadoop):
    plist = []
    nodes =  NODE_LIST + ["loadgen171"]

    # star
    for each in range(num_cstar):
        placement = STRATEGIES["random"](nodes, 6)
        placement["ycsb"] = placement.pop(6)
        plist.append(placement)

    # no star
    for each in range(num_cnonstar):
        placement = STRATEGIES["random"](nodes, 5)
        placement["ycsb"] = placement.pop(5)
        plist.append(placement)

    # hadoop
    for each in range(num_hadoop):
        plist.append(STRATEGIES["random"](nodes, 5))

    return plist

def prepare_macromap_placement():
    plist = []
    pm = {}

    # pm["cassandra:star:placement"] = {1 : "loadgen162",
    #                                   2 : "loadgen171",
    #                                   3 : "loadgen164",
    #                                   4 : "loadgen165",
    #                                   5 : "loadgen166",
    #                                   "ycsb" : "loadgen163"}

    # pm["cassandra:nostar:placement"] = {1 : "loadgen171",
    #                                     2 : "loadgen164",
    #                                     3 : "loadgen165",
    #                                     4 : "loadgen166",
    #                                     "ycsb" : "loadgen167"}

    # pm["hadoop:placement"] = {1 : "loadgen162",
    #                           2 : "loadgen163",
    #                           3 : "loadgen164",
    #                           4 : "loadgen165",
    #                           5 : "loadgen166",
    #                           }

    # LALITH ATTEMPT
    pm["cassandra:star:placement"] = {1 : "loadgen164",
                                      2 : "loadgen165",
                                      3 : "loadgen166",
                                      4 : "loadgen167",
                                      5 : "loadgen167",
                                      "ycsb" : "loadgen163"}

    pm["cassandra:nostar:placement"] = {1 : "loadgen162",
                                        2 : "loadgen163",
                                        3 : "loadgen164",
                                        4 : "loadgen165",
                                        "ycsb" : "loadgen162"}

    pm["hadoop:placement"] = {1 : "loadgen162",
                              2 : "loadgen162",
                              3 : "loadgen167",
                              4 : "loadgen171",
                              5 : "loadgen171",
                              }

    # STEFAN ATTEMPT
    # pm["cassandra:star:placement"] = {1 : "loadgen163",
    #                                   2 : "loadgen165",
    #                                   3 : "loadgen171",
    #                                   4 : "loadgen171",
    #                                   5 : "loadgen166",
    #                                   "ycsb" : "loadgen163"}

    # pm["cassandra:nostar:placement"] = {1 : "loadgen164",
    #                                     2 : "loadgen164",
    #                                     3 : "loadgen167",
    #                                     4 : "loadgen167",
    #                                     "ycsb" : "loadgen164"}

    # pm["hadoop:placement"] = {1 : "loadgen162",
    #                           2 : "loadgen162",
    #                           3 : "loadgen166",
    #                           4 : "loadgen165",
    #                           5 : "loadgen171",
    #                           }


    plist.append(pm["cassandra:star:placement"])
    plist.append(pm["cassandra:nostar:placement"])
    plist.append(pm["hadoop:placement"])
    return plist


if __name__ == '__main__':
    exp_number = 120 # 12 == all quorum, 13 == all one read=one
    # cassandra_baseline_experiment(exp_number)
    # spew_cassandra_experiment(exp_number)
    # cpu_cassandra_experiment(exp_number)
    iperf_cassandra_experiment(exp_number)
    # spew_hadoop_experiment(exp_number)
    # baseline_hadoop_experiment(exp_number)
    # cpu_hadoop_experiment(exp_number)
    # iperf_hadoop_experiment(exp_number)
    # exp_number = 130
    # for i in range(0,5):
    #     l = prepare_random_placement_list(1, 1, 1)
    #     evaluation_experiment(exp_number, 1, 1, 1, l)

    # for i in range(0,5):
    #         l = prepare_random_placement_list(2, 2, 2)
    #         evaluation_experiment(exp_number, 2, 2, 2, l)

    # exp_number = 140
    # l = prepare_macromap_placement()
    # evaluation_experiment(exp_number, 1, 1, 1, l)