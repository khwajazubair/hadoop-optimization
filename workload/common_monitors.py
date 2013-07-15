from interact import *


def run_bwmon(node_list, run_id):

    command = "sudo pkill bmon; sudo bmon -p br100,eth2 -o ascii:header=10 -r 0.01 &> /mnt/bmon-%s" % run_id
    for node in node_list:
        execute_on_lg_bg(node, command)

    return


def retreive_bwmon_results(node_list, run_id):

    for node in node_list:
        print node
        execute_on_lg(node, "sudo pkill bmon")
        scp_file_from_lg("/mnt/bmon-%s" % run_id, "runs/bmon.%s.%s" % (node, run_id), node)

        # cleanup
        execute_on_lg(node, "rm /mnt/bmon-%s" % run_id)


def run_iostat(node_list, run_id):

    command = "sudo pkill iostat; sudo iostat -x -k -d 1 &> /mnt/iostat-%s" % run_id
    for node in node_list:
        execute_on_lg_bg(node, command)

    return


def retreive_iostat_results(node_list, run_id):

    for node in node_list:
        print node
        execute_on_lg(node, "sudo pkill iostat")
        scp_file_from_lg("/mnt/iostat-%s" % run_id, "runs/iostat.%s.%s" % (node, run_id), node)

        # cleanup
        execute_on_lg(node, "rm /mnt/iostat-%s" % run_id)


def run_mpstat(node_list, run_id):

    command = "sudo pkill mpstat; sudo mpstat -P ALL 1 &> /mnt/mpstat-%s" % run_id
    for node in node_list:
        execute_on_lg_bg(node, command)

    return


def retreive_mpstat_results(node_list, run_id):

    for node in node_list:
        print node
        execute_on_lg(node, "sudo pkill mpstat")
        scp_file_from_lg("/mnt/mpstat-%s" % run_id, "runs/mpstat.%s.%s" % (node, run_id), node)

        # cleanup
        execute_on_lg(node, "rm /mnt/mpstat-%s" % run_id)
