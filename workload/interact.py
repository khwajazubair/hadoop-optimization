import subprocess
import time

API_SERVER = "loadgen161"

ACTIVE_MAP = {}
IMAGE_MAP = {}


class VirtualMachine:
    def __init__(self, id, name, ip):
        self.id = id
        self.name = name
        self.ip = ip


def sync_glance_index():
    print "Synchronising glance index"
    for row in execute_locally(["glance", "index"])[2:]:
        lines = row.split()
        id = lines[0]
        display_name = lines[1]
        IMAGE_MAP[display_name] = id


def sync_nova_list():
    print "Synchronising nova list"
    for row in execute_locally(["nova", "list"])[3:-1]:
        lines = row.split()
        id = lines[1]
        display_name = lines[3]
        ip = lines[-2].split('=')[-1]
        ACTIVE_MAP[display_name] = VirtualMachine(id, display_name, ip)


def nova_boot(image_id, node_name, flavor, extra_args):
    cmd_string = "nova boot \
                  --flavor %s \
                  --image %s \
                  --key_name nova_test \
                  %s \
                  --security_group default %s" \
                   % (flavor, image_id, extra_args, node_name)
    print "Executing nova boot. Command string: ", cmd_string
    execute_locally(cmd_string.split())


def nova_delete(image_id):
    sync_nova_list()
    cmd_string = ["nova", "delete", "%s" % (image_id)]
    print "Executing nova delete. Command string: nova delete " + str(image_id)
    execute_locally(cmd_string)


def get_id_for_instance(name):
    sync_nova_list()
    return ACTIVE_MAP[name].id


def get_ip_for_instance(name):
    sync_nova_list()
    return ACTIVE_MAP[name].ip


def execute_locally(command):
    ssh = subprocess.Popen(command,
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    print ssh.stderr.readlines()

    return result


def execute_over_ssh(host, command):
    command = "source ~/creds/openrc; " + command
    ssh = subprocess.Popen(["ssh", "%s" % host, command],
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()

    return result


def execute_on_vm(host, command):
    command = "source ~/.bashrc; " + command
    command = ["ssh", "-i", "/home/routerlab/.ssh/nova_test", "ubuntu@%s" % host, command]
    print command
    ssh = subprocess.Popen(command,
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()

    return result


def scp_file_to_host(filename, host):
    print "scp", "%s" % filename, "%s" % host
    subprocess.Popen(["scp", "-i", "/home/routerlab/.ssh/nova_test", "%s" % filename, "ubuntu@%s" % host],
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    return

 ##### Written by Zubair for facebook traces folder to copy ######
def scp_folder_to_host(foldername, host):
    print "scp", "%s" % foldername, "%s" % host
    subprocess.Popen(["scp", "-r", "-i", "/home/routerlab/.ssh/nova_test", "%s" % foldername, "ubuntu@%s" % host],
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    return



def scp_file_from_host(src, dst, host):
    print "scp", "%s:%s" % (host, src), "%s" % dst
    subprocess.Popen(["scp", "-i", "/home/routerlab/.ssh/nova_test", "ubuntu@%s:%s" % (host, src), "%s" % dst],
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    return


def scp_folder_from_host(src, dst, host):
    print "scp", "%s:%s" % (host, src), "%s" % dst
    subprocess.Popen(["scp", "-r", "-i", "/home/routerlab/.ssh/nova_test", "ubuntu@%s:%s" % (host, src), "%s" % dst],
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    return




def execute_on_vm_bg(host, command):
    command = "source ~/.bashrc; " + command
    command = ["ssh", "-i", "/home/routerlab/.ssh/nova_test", "ubuntu@%s" % host, command]
    print command
    subprocess.Popen(command,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)

    return

def execute_on_lg(host, command):
    command = "source ~/.bashrc; " + command
    command = ["ssh", "-i", "/home/routerlab/.ssh/nova_test", "routerlab@%s" % host, command]
    print command
    ssh = subprocess.Popen(command,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()

    return


def execute_on_lg_bg(host, command):
    command = "source ~/.bashrc; " + command
    command = ["ssh", "-i", "/home/routerlab/.ssh/nova_test", "routerlab@%s" % host, command]
    print command
    subprocess.Popen(command,
                     stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)

    return


def scp_file_from_lg(src, dst, host):
    print ["scp", "-i", "/home/routerlab/.ssh/nova_test", "routerlab@%s:%s" % (host, src), "%s" % dst]
    ssh = subprocess.Popen(["scp", "-i", "/home/routerlab/.ssh/nova_test", "routerlab@%s:%s" % (host, src), "%s" % dst],
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    return result
