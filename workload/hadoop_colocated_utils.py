from interact import *
from placement import *
import time
import sys


def spawn_hadoop_vms(num_hadoop,
                     exp_number,
                     placement_map):
    """ Spawns Hadoop VMs according to placement_map """

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

    sync_glance_index()
    sync_nova_list()

    nova_boot(IMAGE_MAP["hadoop-nn"],
              HADOOP_HOSTNAME_PREFIX + "-1", 4,
              "--availability-zone=nova:%s" % placement_map[1])
    p=2
    for l in range (1, 3):
     for i in range(2, num_hadoop + 1):
        nova_boot(IMAGE_MAP["hadoop-dn-v2"],
                  HADOOP_HOSTNAME_PREFIX + "-%s" % p, 4,
                  "--availability-zone=nova:%s" % placement_map[i])
        p+=1

    # Check to see if all VMs have booted
    retries = 10
    success = True

    while (retries != 0):
        print "Retries ", retries
        success = True
        sync_nova_list()
        for i in range(1, num_hadoop + 1):
            if (not HADOOP_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                success = False
                break

        if (success is False):
            print "Hadoop VMs haven't booted yet. \
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


def setup_hadoop(num_hadoop,
                 exp_number, schedule):
    """ Spawns Hadoop VMs according to placement_map """

    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
    HADOOP_NN = HADOOP_HOSTNAME_PREFIX + "-1"

    sync_nova_list()

    #
    # Setup slaves file
    #
    slaves_file = open('slaves_%s' % (exp_number), 'w')

    for i in range(2, num_hadoop + 1):
        slaves_file.write(HADOOP_HOSTNAME_PREFIX + "-%s" % i + "\n")

    slaves_file.close()

    scp_file_to_host("slaves_%s" % (exp_number), get_ip_for_instance(HADOOP_NN)
                     + ":~/hadoop-3.0.0-SNAPSHOT/etc/hadoop/slaves")
   
    if (schedule == "fair"):
          scp_file_to_host("/home/routerlab/zubair-side-effects/workload/schedulars/yarn-site.xml", get_ip_for_instance(HADOOP_NN)
                     + ":~/hadoop-3.0.0-SNAPSHOT/etc/hadoop")
          scp_file_to_host("/home/routerlab/zubair-side-effects/workload/schedulars/fair-schedular.xml", get_ip_for_instance(HADOOP_NN)
                     + ":~/hadoop-3.0.0-SNAPSHOT/etc/hadoop")
          print "fair schedular is selected"
    elif (schedule =="capacity"):
          print "Default capacity schedular is selected"
    else:
          print" Schedular not specified";
          sys.exit(1) 
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "hdfs namenode -format")
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "~/hadoop-3.0.0-SNAPSHOT/sbin/hadoop-daemon.sh start namenode")
    time.sleep(5)
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "HADOOP_SSH_OPTS='-i  /home/ubuntu/.ssh/hadoop_rsa -l ubuntu' ~/hadoop-3.0.0-SNAPSHOT/sbin/hadoop-daemons.sh start datanode")
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "~/hadoop-3.0.0-SNAPSHOT/sbin/yarn-daemon.sh start resourcemanager")
    time.sleep(5)
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "HADOOP_SSH_OPTS='-i  /home/ubuntu/.ssh/hadoop_rsa -l ubuntu' ~/hadoop-3.0.0-SNAPSHOT/sbin/yarn-daemons.sh start nodemanager")
    execute_on_vm(get_ip_for_instance(HADOOP_NN), "~/hadoop-3.0.0-SNAPSHOT/sbin/./mr-jobhistory-daemon.sh start historyserver")


def hadoop_load_workload(pm, exp_number, workload):
    time_before = time.time()
    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
    HADOOP_NN = HADOOP_HOSTNAME_PREFIX + "-1"

    sync_nova_list()
    load_command = ""
    if (workload == "terasort"):
       print "Loading Hadoop teraSort"
       load_command = "hadoop jar \
       /home/ubuntu/hadoop-3.0.0-SNAPSHOT/share/hadoop/mapreduce/hadoop-*examples*.jar \
               teragen 50000000 /user/hduser/terasort-input"
   
    elif (workload == "fb"):

         ##### Written by Zubair to copy fbTraces to Name Node#####
         ## scp_folder_to_host("/home/routerlab/zubair-side-effects/workload/fbTraces", get_ip_for_instance(HADOOP_NN)
         ##            + ":~/")
         ## print "fbTraces are copied to name node"
         print "Loading Hadoop FaceBook Traces"
         load_command = "rm -rf scriptsTest; \
                         java GenerateReplayScript FB-2009_samples_24_times_1hr_0_first50jobs.tsv 600 7 67108864 10 scriptsTest\
                         /home/ubuntu/workGenInput workGenOutputTest 67108864 /home/ubuntu/logs hadoop /home/ubuntu/WorkGen.jar\
                         '/home/ubuntu/hadoop-3.0.0-SNAPSHOT/myconf/workGenKeyValue_conf.xsl';\
                         hadoop jar HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf hadoop-3.0.0-SNAPSHOT/myconf/randomwriter_conf.xsl\
                         /home/ubuntu/workGenInput;"                          

    else: 
        print "Hadoop Load Failed, workload not recognized (%s)" % (workload)
        sys.exit(1)

    execute_on_vm(get_ip_for_instance(HADOOP_NN), load_command)
    print "Hadoop load terminating after time: " + str(time.time() - time_before)


def hadoop_run_workload(pm, exp_number, workload):
    time_before = time.time()
    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
    HADOOP_NN = HADOOP_HOSTNAME_PREFIX + "-1"

    sync_nova_list()
    run_command = ""
    if (workload == "terasort"): 
       print "Running "+workload+" workload."
       run_command = "hadoop jar /home/ubuntu/hadoop-3.0.0-SNAPSHOT/share/hadoop/mapreduce/hadoop-*examples*.jar terasort \
       /user/hduser/terasort-input /user/hduser/terasort-output &> run.out;python parse_terasort_logs.py"
   
    elif (workload == "fb"):
       print "Running " + workload + " workload."
       run_command = "chmod a+rwx scriptsTest; cd scriptsTest;./run-jobs-all.sh; python parse_facebook_logs.py"
       
       time.sleep(10)
    else:
       print "Running Hadoop Workload " + workload + " Failed."
       sys.exit(1)

    execute_on_vm(get_ip_for_instance(HADOOP_NN), run_command)

    scp_file_from_host("run.out", "runs/run.out.%s.%s" % (exp_number, time_before), get_ip_for_instance(HADOOP_NN))
    scp_file_from_host("logs_summary.out", "runs/logs_summary.out%s.%s" % (exp_number, time_before), get_ip_for_instance(HADOOP_NN))
    #scp_file_from_host("hadoop-3.0.0-SNAMPSHOT/logs/facebook_log.tsv", "runs/facebook_log.tsv.%s.%s" % (exp_number, time_before), get_ip_for_instance(HADOOP_NN))

    scp_file_from_host("hadoop-3.0.0-SNAPSHOT/logs/*history*.log", "runs/history.%s.%s" % (exp_number, time_before), get_ip_for_instance(HADOOP_NN))

    print "Hadoop run terminating after time: " + str(time.time() - time_before)
