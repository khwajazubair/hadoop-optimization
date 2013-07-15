from interact import *
from placement import *
import time
import sys


def spawn_web_vms(exp_number,
                  placement_map):
    """ Spawns Web VMs according to placement_map """

    WEB_HOSTNAME_PREFIX = "web-%s" % exp_number
    NUM_WEB = 3

    sync_glance_index()
    sync_nova_list()

    #
    # Delete all existing instances of web vms
    #
    for i in range(1, NUM_WEB + 1):
        if (WEB_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
            nova_delete(ACTIVE_MAP[WEB_HOSTNAME_PREFIX + "-%s" % i].id)

    time.sleep(10)

    sync_glance_index()
    sync_nova_list()

    for i in range(1, 4):
        nova_boot(IMAGE_MAP["web-z-%s" % (i)],
                  WEB_HOSTNAME_PREFIX + "-%s" % (i), 4,
                  "--availability-zone=nova:%s" % placement_map[i])

    # Check to see if all VMs have booted
    time.sleep(120)
    retries = 10
    success = True

    while (retries != 0):
        print "Retries ", retries
        success = True
        sync_nova_list()
        for i in range(1, NUM_WEB + 1):
            if (not WEB_HOSTNAME_PREFIX + "-%s" % i in ACTIVE_MAP):
                success = False
                break

        if (success is False):
            print "Web VMs haven't booted yet. \
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


def setup_web(exp_number):
    """ Spawns Hadoop VMs according to placement_map """

    WEB_HOSTNAME_PREFIX = "web-%s" % exp_number
    WEB_CLIENT = WEB_HOSTNAME_PREFIX + "-1"
    WEB_DATABASE = WEB_HOSTNAME_PREFIX + "-2"
    WEB_SERVER = WEB_HOSTNAME_PREFIX + "-3"

    sync_nova_list()

    execute_on_vm(get_ip_for_instance(WEB_CLIENT), "/home/ubuntu/web-release/faban/master/bin/startup.sh")
    execute_on_vm(get_ip_for_instance(WEB_DATABASE), "/home/ubuntu/web-release/apache-tomcat-6.0.35/bin/startup.sh")
    execute_on_vm_bg(get_ip_for_instance(WEB_DATABASE), "cd web-release/mysql-5.5.20-linux2.6-x86_64; bin/mysqld_safe --defaults-file=/etc/my.cnf --user=mysql")
    time.sleep(50)
    execute_on_vm(get_ip_for_instance(WEB_SERVER), "sudo /usr/local/nginx/sbin/nginx")
    execute_on_vm(get_ip_for_instance(WEB_SERVER), "sudo /usr/local/sbin/php-fpm")


#def hadoop_load_workload(pm, exp_number, workload):
#    time_before = time.time()
#    HADOOP_HOSTNAME_PREFIX = "hadoop-%s" % exp_number
#    HADOOP_NN = HADOOP_HOSTNAME_PREFIX + "-1"

#    sync_nova_list()
#    load_command = ""
#    if (workload == "teraSort"):
#       print "Loading Hadoop teraSort"
#       load_command = "hadoop jar \
#       /home/ubuntu/hadoop-3.0.0-SNAPSHOT/share/hadoop/mapreduce/hadoop-*examples*.jar \
#               teragen 50000000 /user/hduser/terasort-input"
#   
#    elif (workload == "fb"):
#
#         ##### Written by Zubair to copy fbTraces to Name Node#####
#         ## scp_folder_to_host("/home/routerlab/zubair-side-effects/workload/fbTraces", get_ip_for_instance(HADOOP_NN)
#         ##            + ":~/")
#         ## print "fbTraces are copied to name node"
#         print "Loading Hadoop FaceBook Traces"
#         load_command = "rm -rf scriptsTest; \
#                         java GenerateReplayScript FB-2009_samples_24_times_1hr_0_first50jobs.tsv 600 2 67108864 10 scriptsTest\
#                         /home/ubuntu/workGenInput workGenOutputTest 67108864 /home/ubuntu/workGenLogs hadoop /home/ubuntu/WorkGen.jar\
#                         '/home/ubuntu/hadoop-3.0.0-SNAPSHOT/myconf/workGenKeyValue_conf.xsl';\
#                         hadoop jar HDFSWrite.jar org.apache.hadoop.examples.HDFSWrite -conf hadoop-3.0.0-SNAPSHOT/myconf/randomwriter_conf.xsl\
#                         /home/ubuntu/workGenInput"
#                          
#
#    else: 
#        print "Hadoop Load Failed, workload not recognized (%s)" % (workload)
#        sys.exit(1)
#
#    execute_on_vm(get_ip_for_instance(HADOOP_NN), load_command)
#    print "Hadoop load terminating after time: " + str(time.time() - time_before)


def web_run_workload(pm, exp_number):
    time_before = time.time()
    WEB_HOSTNAME_PREFIX = "web-%s" % exp_number
    WEB_CLIENT = WEB_HOSTNAME_PREFIX + "-1"

    sync_nova_list()
    run_command = ""
       
    print "Running Faban Master workload."
    run_command = " cd web-release/apache-olio-php-src-0.2/workload/php/trunk/deploy;\
                    $FABAN_HOME/bin/fabancli submit OlioDriver default run.xml"
   
    execute_on_vm(get_ip_for_instance(WEB_CLIENT), run_command)

    #scp_folder_from_host("web-release/faban/output/*", "runs/olio-log.%s.%s" % (exp_number, time_before), get_ip_for_instance(WEB_CLIENT))

    #print "WebService benchmark run terminating after time: " + str(time.time() - time_before)
