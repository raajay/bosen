import os
import sys
import random

if (len(sys.argv) != 2):
    print "usage: ./script/background.py <num_comms>"

busy_hosts = ["node-%d" % x for x in range(3, 21)]
num_comms = int(sys.argv[1])

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )

def pick_random_pair():
    if(len(busy_hosts) == 1):
        print "only one host avl."
        return

    src = random.choice(busy_hosts)
    dst = random.choice(busy_hosts)
    while src == dst:
        dst = random.choice(busy_hosts)
        pass
    return (src, dst)


def pick_src_dst_pairs(count):
    return [pick_random_pair() for x in range(0, count)]


def launch_server(host):
    cmd = ssh_cmd + host + " "
    cmd += "\'/media/raajay/iperf_server.sh\'"
    print cmd
    os.system(cmd)


def launch_client(host, server):
    cmd = ssh_cmd + host + " "
    cmd += "\'/media/raajay/iperf_client.sh %s\'" % server
    print cmd
    os.system(cmd)


xx = pick_src_dst_pairs(num_comms)
# print xx
for x in xx:
    print x[0]
    launch_server(x[0])
    print x[1]
    launch_client(x[1], x[0])
