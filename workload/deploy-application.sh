NODE_USERNAME=routerlab
NODE_NAME_PREFIX=loadgen
CONTROLLER_NODE_NUMBER=161
IMAGE_ID="531916e2-c007-420f-9501-23e43e96fc17" # Ubuntu 12.04 server cloud image
EXEC="ssh $NODE_USERNAME@${NODE_NAME_PREFIX}${CONTROLLER_NODE_NUMBER}"

host_list () {
    echo "----------List of hosts---------"
    $EXEC "source creds/openrc; nova hypervisor-list" \
    | grep $NODE_NAME_PREFIX | awk {'print $4'} | sed 's/.routerlab//'
}

vm_mapping_list () {
    echo "----------VM to host mappings---------"
    for host in `host_list | grep $NODE_NAME_PREFIX`; do
        # ssh $NODE_USERNAME@$host ""
        echo "----" $host

        echo "select display_name from instances where host='$host' and vm_state='ACTIVE';" \
        | ssh $NODE_USERNAME@loadgen161 mysql -u root -prouterlab nova | grep -v "display_name"

        echo ""

    done
}

launch_vm_on_host () {
    if [ $# -ne 3 ];
        then
        echo "ERROR: launch-vm needs 3 arguments"
        echo "USAGE: launch-vm <node-number> <name-prefix> <flavor>"
        exit 1
    fi
    echo "Launching VM on $1 with name $2-$1"

    $EXEC "source creds/openrc; nova boot --flavor $3 --image $IMAGE_ID\
      --key_name nova_test \
      --availability-zone=nova:$1 \
      --security_group default $2-$1"
}

launch_vm () {
    if [ $# -ne 2 ];
        then
        echo "ERROR: launch-vm needs 2 argument"
        echo "USAGE: launch-vm <name-prefix> <flavor>"
        exit 1
    fi
    echo "Launching VM with name $1"

    $EXEC "source creds/openrc; nova boot --flavor $2 --image $IMAGE_ID\
      --key_name nova_test \
      --security_group default $1"
}

delete_vm () {

    for id in `$EXEC "source creds/openrc; nova list | grep $1 " | awk '{print $2}'`;
    do
      $EXEC "source creds/openrc; nova delete $id"
    done
}

delete_all_vm () {
   for id in `$EXEC "source creds/openrc; nova list" | grep $1 | awk '{print $2}'`;
    do
      #$EXEC "nova delete
      echo $id
      $EXEC "source creds/openrc; nova delete $id"
    done
}

case $1 in
    "host-list" )
        host_list;;
    "vm-mapping-list" )
        vm_mapping_list;;
    "launch-vm-on-host" )
        launch_vm_on_host $2 $3 $4;;
    "launch-vm" )
        launch_vm $2 $3;;
    "delete-vm" )
        delete_vm $2;;
    "delete-all-vms" )
        delete_all_vm $2;;
esac
