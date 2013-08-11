from interact import *
from placement import *
from cassandra_utils import *
from resource_bench_utils import *
import hadoop_utils
import hadoop_utils_2
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
    spawnProcessList = []
    hadoopSpawnProcess1 = Process (target=hadoop_utils.spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"],
                     pm["hadoop:placement"]))
    hadoopSpawnProcess1.start()
    spawnProcessList.append(hadoopSpawnProcess1)

    hadoopSpawnProcess2 = Process (target=hadoop_utils_2.spawn_hadoop_vms(pm["hadoop:num_hadoop"],
                     pm["exp_number"]+1,
                     pm["hadoop:placement"]))
    hadoopSpawnProcess2.start()
    spawnProcessList.append(hadoopSpawnProcess2)

    for process in spawnProcessList:
        process.join()


    time.sleep(120)

    ####### START PROCESSES ##########
    startProcessList = []

    hadoopStartProcess1 = Process(target=hadoop_utils.setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"],pm, schedule))
    hadoopStartProcess1.start()
    startProcessList.append(hadoopStartProcess1)
    
    hadoopStartProcess2 = Process(target=hadoop_utils_2.setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]+1,pm, schedule))
    hadoopStartProcess2.start()
    startProcessList.append(hadoopStartProcess2)

    for process in startProcessList:
        process.join()
    #time.sleep(120)





    ####### LOAD DATA ##########
    loadProcessList = []
    hadoopLoadProcess1 = Process(target=hadoop_utils.hadoop_load_workload, args=(pm, pm["exp_number"], workload ))
    hadoopLoadProcess1.start()
    #hadoopLoadProcess.join()
    #time.sleep(30)
    loadProcessList.append(hadoopLoadProcess1)
    
    hadoopLoadProcess2 = Process(target=hadoop_utils_2.hadoop_load_workload, args=(pm, pm["exp_number"]+1, workload ))
    hadoopLoadProcess2.start()
    #hadoopLoadProcess.join()
    #time.sleep(30)
    loadProcessList.append(hadoopLoadProcess2)

    for process in loadProcessList:
        process.join()




    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])

    runProcessList = [] 
    hadoopRunProcess1 = Process(target=hadoop_utils.hadoop_run_workload, args=(pm, pm["exp_number"], workload))
    #time.sleep(40)

    hadoopRunProcess1.start()
    runProcessList.append(hadoopRunProcess1)


    hadoopRunProcess2 = Process(target=hadoop_utils_2.hadoop_run_workload, args=(pm, pm["exp_number"]+1, workload))
    #time.sleep(40)

    hadoopRunProcess2.start()
    runProcessList.append(hadoopRunProcess2)

    for process in runProcessList:
        process.join()

    #hadoopRunProcess.join()
    #time.sleep(180)
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def baseline_hadoop_experiment(exp_number):
    

    
    pm = {}
    reducer = int(1)
    pm["exp_number"] = exp_number
    

    workloads = ['terasort']
    load = "terasort"
    schedular = ['capacity', 'fair']
    for schedule in schedular:
    	for hadoop_spec in ["mapred-site-spec-true.xml", "mapred-site-spec-false.xml"]:
             for run_no in range(1,6):

                    files = glob.glob('runs/*')
                    for f in files:
                        os.remove(f) 

                    files = glob.glob('runs_2/*')
                    for f in files:
                        os.remove(f)  
 
                    ################# One-to-One #############
                    pm["hadoop:reducer"]  = reducer
                    pm["hadoop:schedular"]  = schedule
                    pm["mapred-site.xml"] = hadoop_spec
                    pm["hadoop:num_hadoop"] = len(NODE_LIST)
                    pm["hadoop:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
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


                    shutil.copytree("runs", "hadoop-colocate-clusters-runs/runs-%s-%s-%s" % ("colc_clusters-hadoop-experiment-"+schedule_name+"-"\
                                    +spec+"-"+str(reducer), exp_number, int(time.time())))

                    shutil.copytree("runs_2", "hadoop-colocate-clusters-runs/runs-%s-%s-%s" % ("colc_clusters-hadoop-experiment-"+schedule_name+"-"\
                                    +spec+"-"+str(reducer), exp_number+1, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f) 

                    files = glob.glob('runs_2/*')

                    for f in files:
                        os.remove(f) 
        #index= index+1

 
if __name__ == '__main__':
    exp_number = 220 # 12 == all quorum, 13 == all one read=one
    baseline_hadoop_experiment(exp_number)
