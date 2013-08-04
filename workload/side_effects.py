from interact import *
from placement import *
from cassandra_utils import *
from resource_bench_utils import *
import hadoop_utils
import simplejson
import time
import glob
import os
import shutil
from common_monitors import *
from multiprocessing import Process

NODE_LIST = ["loadgen162", "loadgen163", "loadgen164", "loadgen165", "loadgen166", "loadgen167", "loadgen168"]


def run_hadoop_baseline(pm, nodes_used, workload, schedule):
    ''' This is a single run to gather the baseline resource
        and performance profile for hadoop  '''

    ###### SPAWN VMS ##########
    hadoop_utils.spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=hadoop_utils.setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"],pm, schedule))
    hadoopStartProcess.start()
    hadoopStartProcess.join()
    #time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_utils.hadoop_load_workload, args=(pm, pm["exp_number"], workload ))
    hadoopLoadProcess.start()
    hadoopLoadProcess.join()
    #time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])
    hadoopRunProcess = Process(target=hadoop_utils.hadoop_run_workload, args=(pm, pm["exp_number"], workload))
    #time.sleep(40)

    hadoopRunProcess.start()
    hadoopRunProcess.join()
    #time.sleep(180)
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def baseline_hadoop_experiment(exp_number):
    

    
    pm = {}
    index = int(0)
    pm["exp_number"] = exp_number
    workloads = ['terasort']
    schedular = ['capacity', 'fair']
    for schedule in schedular:
    	for load in workloads:
             for run_no in range(1,6):

                    files = glob.glob('runs/*')
                    for f in files:
                        os.remove(f) 
 
                    ################# One-to-One #############
                    pm["hadoop:schedular"]  = schedular[index]
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                    run_hadoop_baseline(pm, NODE_LIST, load, schedule)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("baseline-hadoop-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f) 
        index= index+1

 
if __name__ == '__main__':
    exp_number = 80 # 12 == all quorum, 13 == all one read=one
    baseline_hadoop_experiment(exp_number)
