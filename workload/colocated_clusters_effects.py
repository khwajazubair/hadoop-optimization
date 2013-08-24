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
    loadProcessList.append(hadoopLoadProcess1)
    
    hadoopLoadProcess2 = Process(target=hadoop_utils_2.hadoop_load_workload, args=(pm, pm["exp_number"]+1, workload ))
    hadoopLoadProcess2.start()
    loadProcessList.append(hadoopLoadProcess2)

    for process in loadProcessList:
        process.join()




    ####### RUN WORKLOAD ##########
    run_mpstat(nodes_used, pm["exp_number"])
    run_bwmon(nodes_used, pm["exp_number"])
    run_iostat(nodes_used, pm["exp_number"])

    runProcessList = [] 
    hadoopRunProcess1 = Process(target=hadoop_utils.hadoop_run_workload, args=(pm, pm["exp_number"], workload))
    hadoopRunProcess1.start()
    runProcessList.append(hadoopRunProcess1)


    hadoopRunProcess2 = Process(target=hadoop_utils_2.hadoop_run_workload, args=(pm, pm["exp_number"]+1, workload))
    hadoopRunProcess2.start()
    runProcessList.append(hadoopRunProcess2)

    for process in runProcessList:
        process.join()

   
    retreive_mpstat_results(nodes_used, pm["exp_number"])
    retreive_bwmon_results(nodes_used, pm["exp_number"])
    retreive_iostat_results(nodes_used, pm["exp_number"])


def delete_hadoop_vms(num_hadoop,
                     exp_number,
                     placement_map):
    """ Delete Hadoop VMs according to placement_map """

    #HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    #
    # Delete all existing instances of cassandra and ycsb
    #
    for p in range(1,3):
        HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
        for i in range(1, num_hadoop + 1):
            if (HADOOP_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                nova_delete(ACTIVE_MAP[HADOOP_HOSTNAME_PREFIX + "-%s" % i].id)

            time.sleep(20)

        #Next Experiment    
        exp_number = exp_number + 1


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
                    try:
                        shutil.rmtree("runs")
                    except OSError:
                            print "runs/ folder doesn't exist. Continuing."

                    try:
                        shutil.rmtree("runs_2")
                    except OSError:
                            print "runs_2/ folder doesn't exist. Continuing."

                    os.mkdir("runs")
                    os.mkdir("runs_2")

 
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

                    config_dump = open('runs_2/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    schedule_name= str(schedule)
                    if pm["mapred-site.xml"] == "mapred-site-spec-false.xml" :
                        spec = "spec-false"
                    else:
                        spec = "spec-true"


                    shutil.copytree("runs", "exp_4/hadoop_co_cl1/runs-%s-%s-%s" % ("co-cl-hadoop-exp-"+str(run_no)+"-"+schedule_name+"-"\
                                    +spec+"-"+str(pm["hadoop:reducer"]), exp_number, int(time.time())))

                    shutil.copytree("runs_2", "exp_4/hadoop_co_cl2/runs-%s-%s-%s" % ("co-cl-hadoop-exp-"+str(run_no)+"-"+schedule_name+"-"\
                                    +spec+"-"+str(pm["hadoop:reducer"]), exp_number+1, int(time.time())))
                    time.sleep(10)

                    shutil.rmtree("runs")
                    shutil.rmtree("runs_2")
                    time.sleep(10)
                    
                    delete_hadoop_vms(pm["hadoop:num_hadoop"],
                            pm["exp_number"],
                            pm["hadoop:placement"])
                        
        #index= index+1

 
if __name__ == '__main__':
    exp_number = 998 # 12 == all quorum, 13 == all one read=one
    baseline_hadoop_experiment(exp_number)
