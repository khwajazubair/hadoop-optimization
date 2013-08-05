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
        and performance profile for Cassandra  '''

    ###### SPAWN VMS ##########
    hadoop_utils.spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"])
    time.sleep(120)

    ####### START PROCESSES ##########

    hadoopStartProcess = Process(target=hadoop_utils.setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"],pm, schedule))
    hadoopStartProcess.start()
    hadoopStartProcess.join()
    time.sleep(120)

    ####### LOAD DATA ##########

    hadoopLoadProcess = Process(target=hadoop_utils.hadoop_load_workload, args=(pm, pm["exp_number"], workload ))
    hadoopLoadProcess.start()
    hadoopLoadProcess.join()
    time.sleep(30)

    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])
    hadoopRunProcess = Process(target=hadoop_utils.hadoop_run_workload, args=(pm, pm["exp_number"], workload))
    time.sleep(40)

    hadoopRunProcess.start()
    hadoopRunProcess.join()
    time.sleep(180)
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def baseline_hadoop_experiment(exp_number):

    pm = {}
    #index = int(0)
    pm["exp_number"] = exp_number
    #workloads = ['terasort']
    load = "terasort"
    schedular = ['capacity','fair']
    for schedule in schedular:
    	 for hadoop_spec in ["mapred-site-spec-true.xml", "mapred-site-spec-false.xml"]:
             for i in range (1,6):
        	    files = glob.glob('runs/*')
           	    for f in files:
                        os.remove(f)
                    ################# One-to-One #############
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:schedular"]  = schedule
                    pm["mapred-site.xml"] = hadoop_spec
                    #pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                    pm["hadoop:placement"] = {1: "loadgen162", 2: "loadgen163", 3: "loadgen163", 4: "loadgen164", 5: "loadgen164",\
                                              6: "loadgen165", 7 :"loadgen165",} 
                    run_hadoop_baseline(pm, NODE_LIST, load, schedule)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()
                    schedule_name= str(schedule)
                    if pm["mapred-site.xml"] == "mapred-site-spec-false.xml" :
                        spec = "spec-false"
                    else:
                        spec = "spec-true"

                    shutil.copytree("runs", "hadoop-colocated-runs/runs-%s-%s-%s" % ("colocated-hadoop-experiment-"+schedule_name+"-"+spec, exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)
        #index=index+1
        
 
if __name__ == '__main__':
    exp_number = 110 # 12 == all quorum, 13 == all one read=one
    baseline_hadoop_experiment(exp_number)
