import random

# This file includes all the placement strategies for the experiments


def one_to_one_strategy(pm_list, vm_count):
    if (len(pm_list) < vm_count):
        print "ERROR: Not enough physical machines \
        to satisfy one-to-one strategy"
        return

    output_map = {}

    for i in range(vm_count):
        output_map[i + 1] = pm_list[i]

    return output_map


def round_robin_strategy(pm_list, vm_count):

    output_map = {}
    for i in range(vm_count):
        output_map[i + 1] = pm_list[i % (len(pm_list))]

    return output_map


def random_strategy(pm_list, vm_count):
    output_map = {}
    for i in range(vm_count):
        output_map[i + 1] = pm_list[random.randint(0, len(pm_list) - 1)]

    return output_map


STRATEGIES = {
    "one-to-one": one_to_one_strategy,
    "round-robin": round_robin_strategy,
    "random" : random_strategy,
}
