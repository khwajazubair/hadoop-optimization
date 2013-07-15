from interact import *
from placement import *
from cassandra_utils import *
from hadoop_utils import *
from resource_bench_utils import *
from random import shuffle
import time
import glob
import os
import shutil
from common_monitors import *
from multiprocessing import Process

NUM_CASSANDRA = 6
NUM_YCSB = 2
NODE_LIST = ["loadgen162", "loadgen163", "loadgen164", "loadgen165", "loadgen166", "loadgen167"]


# def run_cassandra_experiment(num_cassandra, num_ycsb, exp_number, property_map):

#     spawn_cassandra_vms(num_cassandra, num_ycsb, exp_number, property_map["cassandra:placement"])
#     time.sleep(80)
#     cassandra_setup(num_cassandra, exp_number)

#     cassandra_run_workload(num_cassandra, num_ycsb, exp_number, property_map)


def run_hadoop_experiment(pm, nodes_used):

    ####### SPAWN VMS ##########
    spawn_cassandra_vms(pm["cassandra:num_cassandra"],
                        pm["cassandra:num_ycsb"],
                        pm["exp_number"],
                        pm["cassandra:placement"])
    spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    cassandraStartProcess = Process(target=cassandra_setup, args=(pm["cassandra:num_cassandra"], pm["exp_number"]))
    cassandraStartProcess.start()
    hadoopStartProcess = Process(target=setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
    hadoopStartProcess.start()
    cassandraStartProcess.join()
    hadoopStartProcess.join()

    time.sleep(120)

    ####### LOAD DATA ##########

    cassandraLoadProcess = Process(target=cassandra_load_workload,
                                   args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    hadoopLoadProcess = Process(target=hadoop_load_workload, args=(pm,))
    cassandraLoadProcess.start()
    hadoopLoadProcess.start()
    cassandraLoadProcess.join()
    hadoopLoadProcess.join()

    time.sleep(30)

    ####### RUN WORKLOAD ##########

    run_bwmon(nodes_used, pm["cassandra:experimentid"])
    run_iostat(nodes_used, pm["cassandra:experimentid"])
    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    hadoopRunProcess = Process(target=hadoop_run_workload, args=(pm,))
    cassandraRunProcess.start()
    hadoopRunProcess.start()
    cassandraRunProcess.join()
    hadoopRunProcess.join()
    time.sleep(10)
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])


def run_cassandra_experiment(pm, nodes_used):

    ####### SPAWN VMS ##########
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
    cassandraRunProcess = Process(target=cassandra_run_workload,
                                  args=(pm["cassandra:num_cassandra"], pm["cassandra:num_ycsb"], pm["exp_number"], pm))
    cassandraRunProcess.start()
    cassandraRunProcess.join()
    time.sleep(10)
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])


def run_cassandra_iperf_experiment(pm, nodes_used):

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
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])



def run_cassandra_spew_experiment(pm, nodes_used):

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
    retreive_bwmon_results(nodes_used, pm["cassandra:experimentid"])
    retreive_iostat_results(nodes_used, pm["cassandra:experimentid"])


def cassandra_hadoop_experiment():

    property_map = {}
    property_map["exp_number"] = 4
    property_map["cassandra:num_cassandra"] = NUM_CASSANDRA
    property_map["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [200]:
            for workload in ["workload_read_only"]:  # , "workloadb", "workloadc", "workloadd", "workloade", "workloadf"]:

                property_map["cassandra:threads"] = n_threads
                property_map["cassandra:workload"] = workload

                ################# One-to-One #############
                property_map["cassandra:experimentid"] = "OneToOne"
                shuffle(NODE_LIST)
                property_map["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                property_map["hadoop:num_hadoop"] = len(NODE_LIST)
                property_map["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                run_hadoop_experiment(property_map, NODE_LIST)
                # run_cassandra_experiment(property_map, NODE_LIST)

                ############## TWO-to-ONE ##################
                property_map["cassandra:experimentid"] = "TwoToOne"
                node_list = ["loadgen165", "loadgen166", "loadgen163"]
                shuffle(node_list)
                property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                property_map["hadoop:num_hadoop"] = len(node_list)
                property_map["hadoop:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                run_hadoop_experiment(property_map, node_list)
                # run_cassandra_experiment(property_map, NODE_LIST)

                # # ############### Three-to-one ###################

                property_map["cassandra:experimentid"] = "ThreeToOne"
                node_list = ["loadgen165", "loadgen166"]
                shuffle(node_list)
                property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                property_map["hadoop:num_hadoop"] = len(node_list)
                property_map["hadoop:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                run_hadoop_experiment(property_map, node_list)
                run_cassandra_experiment(property_map, NODE_LIST)


def iperf_cassandra_experiment(exp_number):

    property_map = {}
    property_map["exp_number"] = exp_number
    property_map["cassandra:num_cassandra"] = NUM_CASSANDRA
    property_map["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [200]:
            for workload in ["workload_read_only"]:

                property_map["cassandra:threads"] = n_threads
                property_map["cassandra:workload"] = workload

                ################# One-to-One #############
                property_map["cassandra:experimentid"] = "OneToOne"
                shuffle(NODE_LIST)
                property_map["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                property_map["iperf:num_iperf"] = len(NODE_LIST)
                property_map["iperf:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                run_cassandra_iperf_experiment(property_map, NODE_LIST)

                ################# Two-to-One #############

                property_map["cassandra:experimentid"] = "TwoToOne"
                node_list = ["loadgen165", "loadgen166", "loadgen163"]
                shuffle(node_list)
                property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                property_map["iperf:num_iperf"] = len(node_list)
                property_map["iperf:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                run_cassandra_iperf_experiment(property_map, node_list)

                # # ############### Three-to-one ###################

                property_map["cassandra:experimentid"] = "ThreeToOne"
                node_list = ["loadgen165", "loadgen166"]
                shuffle(node_list)
                property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                property_map["iperf:num_iperf"] = len(node_list)
                property_map["iperf:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                run_cassandra_iperf_experiment(property_map, node_list)



def spew_cassandra_experiment(exp_number):

    property_map = {}
    property_map["exp_number"] = exp_number
    property_map["cassandra:num_cassandra"] = NUM_CASSANDRA
    property_map["cassandra:num_ycsb"] = NUM_YCSB

    for run_no in range(1, 2):
        for n_threads in [50]:
            for workload in ["workload_read_only"]:
                for spew_run_params in ["--write -b 256k 22g",
                                        "--write -r -b 256k 20g",
                                        "--write -b 16k 15g",
                                        "--write -r -b 16k 15g",]:

                    files = glob.glob('runs/*')
                    for f in files:
                        os.remove(f)

                    property_map["spew:load_params"] = "--help"
                    property_map["spew:run_params"] = spew_run_params
                    property_map["cassandra:threads"] = n_threads
                    property_map["cassandra:workload"] = workload

                    ################# One-to-One #############
                    property_map["cassandra:experimentid"] = "OneToOne"
                    shuffle(NODE_LIST)
                    property_map["cassandra:placement"] = STRATEGIES["one-to-one"](NODE_LIST, NUM_CASSANDRA)
                    property_map["spew:num_spew"] = len(NODE_LIST)
                    property_map["spew:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                    run_cassandra_spew_experiment(property_map, NODE_LIST)


                    shutil.copytree("runs", "runs-%s-%s-%s-%s" % ("spew-read-cassandra-experiment", exp_number, ":".join(spew_run_params.split()), int(time.time())))


                    files = glob.glob('runs/*')
                    
                    for f in files:
                        os.remove(f)

                    ################ Two-to-One #############

                    property_map["cassandra:experimentid"] = "TwoToOne"
                    node_list = ["loadgen165", "loadgen166", "loadgen163"]
                    shuffle(node_list)
                    property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                    property_map["spew:num_spew"] = len(node_list)
                    property_map["spew:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                    run_cassandra_spew_experiment(property_map, node_list)


                    shutil.copytree("runs", "runs-%s-%s-%s-%s" % ("spew-read-cassandra-experiment", exp_number, ":".join(spew_run_params.split()), int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)

                    # # ############### Three-to-one ###################

                    property_map["cassandra:experimentid"] = "ThreeToOne"
                    node_list = ["loadgen165", "loadgen166"]
                    shuffle(node_list)
                    property_map["cassandra:placement"] = STRATEGIES["round-robin"](node_list, NUM_CASSANDRA)
                    property_map["spew:num_spew"] = len(node_list)
                    property_map["spew:placement"] = STRATEGIES["round-robin"](node_list, len(node_list))
                    run_cassandra_spew_experiment(property_map, node_list)
                    
                    shutil.copytree("runs", "runs-%s-%s-%s-%s" % ("spew-read-cassandra-experiment", exp_number, ":".join(spew_run_params.split()), int(time.time())))




if __name__ == '__main__':
    # Cleanup runs-folder
    exp_number = 123123
    spew_cassandra_experiment(exp_number)