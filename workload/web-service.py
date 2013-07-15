from interact import *
from placement import *
from cassandra_utils import *
from resource_bench_utils import *
import web_utils 
import simplejson
import time
import glob
import os
import shutil
from common_monitors import *
from multiprocessing import Process

NODE_LIST = ["loadgen162", "loadgen163", "loadgen164"]


def run_web_baseline(pm, nodes_used):
    ''' This is a single run to gather the baseline resource
        and performance profile for Cassandra  '''

    ###### SPAWN VMS ##########
    web_utils.spawn_web_vms(pm["exp_number"],
                               pm["web:placement"])
    print "spawning VMs"
    time.sleep(200)
   


    hadoopStartProcess = Process(target=web_utils.setup_web, args=(pm["exp_number"],))
    hadoopStartProcess.start()
    hadoopStartProcess.join()
    time.sleep(20)
    webRunProcess = Process(target=web_utils.web_run_workload, args=(pm, pm["exp_number"]))

    webRunProcess.start()
    webRunProcess.join()


    ####### START PROCESSES ##########
#
#    hadoopStartProcess = Process(target=hadoop_utils.setup_hadoop, args=(pm["hadoop:num_hadoop"], pm["exp_number"]))
#    hadoopStartProcess.start()
#    hadoopStartProcess.join()
#    time.sleep(120)
#
#    ####### LOAD DATA ##########
#
#    hadoopLoadProcess = Process(target=hadoop_utils.hadoop_load_workload, args=(pm, pm["exp_number"] ))
#    hadoopLoadProcess.start()
#    hadoopLoadProcess.join()
#    time.sleep(30)
#
#    ####### RUN WORKLOAD ##########
#    run_mpstat(nodes_used, pm["exp_number"])
#    run_bwmon(nodes_used, pm["exp_number"])
#    run_iostat(nodes_used, pm["exp_number"])
#    hadoopRunProcess = Process(target=hadoop_utils.hadoop_run_workload, args=(pm, pm["exp_number"])
#    time.sleep(40)
#
#    hadoopRunProcess.start()
#    hadoopRunProcess.join()
#    time.sleep(180)
#    retreive_mpstat_results(nodes_used, pm["exp_number"])
#    retreive_bwmon_results(nodes_used, pm["exp_number"])
#    retreive_iostat_results(nodes_used, pm["exp_number"])


def baseline_web_experiment(exp_number):

    pm = {}
    pm["exp_number"] = exp_number
    for run_no in range(1, 2):
        	    files = glob.glob('runs/*')
           	    for f in files:
                        os.remove(f)
                    ################# One-to-One #############
                    pm["web:placement"] = STRATEGIES["round-robin"](NODE_LIST, len(NODE_LIST))
                    run_web_baseline(pm, NODE_LIST)

                    ### Dump the property map to a config file
                    config_dump = open('runs/conf.exp', 'w')
                    config_dump.write(simplejson.dumps(pm, indent=4))
                    config_dump.close()

                    shutil.copytree("runs", "runs-%s-%s-%s" % ("baseline-web-experiment", exp_number, int(time.time())))

                    files = glob.glob('runs/*')

                    for f in files:
                        os.remove(f)

 
if __name__ == '__main__':
    exp_number = 402
    baseline_web_experiment(exp_number)
