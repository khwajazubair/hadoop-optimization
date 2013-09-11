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
    time.sleep(5)

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




def delete_hadoop_vms(num_hadoop,
                     exp_number,
                     placement_map):
    """ Delete Hadoop VMs according to placement_map """

    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number

    sync_glance_index()
    sync_nova_list()

    #
    # Delete all existing instances of cassandra and ycsb
    #
    for i in range(1, num_hadoop + 1):
        if (HADOOP_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[HADOOP_HOSTNAME_PREFIX + "-%s" % i].id)

    time.sleep(20)



def baseline_hadoop_experiment(exp_number):
    

    
    pm = {}
    reducer = int(10)
    pm["exp_number"] = exp_number
    

    workloads = ['terasort']
    load = "terasort"
    #schedular = ['capacity','fair']
    schedular = ['capacity']
    for schedule in schedular:     
    	    for hadoop_spec in ["mapred-site-spec-true.xml","mapred-site-spec-false.xml"]:
                for run_no in range(1,6):

                    try:
                        shutil.rmtree("runs")
                    except OSError:
                            print "runs/ folder doesn't exist. Continuing."

                    os.mkdir("runs")
 
                    ################# One-to-One #############
                    pm["hadoop:reducer"]  = reducer
                    pm["hadoop:schedular"]  = schedule
                    pm["mapred-site.xml"] = "mapred-site-spec-true.xml"
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


                    shutil.copytree("runs", "exp_x/hadoop_bs_3/runs-%s-%s-%s" % ("bs-exp-"+schedule_name+"-"\
                                    +spec+"-"+str(run_no)+"-"+str(reducer), pm["exp_number"], int(time.time())))
                    #shutil.copytree("runs", "exp_x/hadoop_bs_3/runs-%s-%s-%s" % ("bs-exp-"+schedule_name+"-"\
                    #                +spec+"-"+str(reducer), pm["exp_number"], int(time.time())))


                    shutil.rmtree("runs")
                    delete_hadoop_vms(pm["hadoop:num_hadoop"],
                        pm["exp_number"],
                        pm["hadoop:placement"])
        #index= index+1

 
if __name__ == '__main__':
    exp_number = 300 # 12 == all quorum, 13 == all one read=one
    baseline_hadoop_experiment(exp_number)
