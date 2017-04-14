#!/usr/bin/env python

import os, sys
from joblib import Parallel, delayed
import multiprocessing

if len(sys.argv) != 2:
  print "usage: %s <hostfile>" % sys.argv[0]
  sys.exit(1)

host_file = sys.argv[1]
prog_name = "DNN"

# Get host IPs
with open(host_file, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]
# there can be multiple clients on each machine
# so we remove duplicates by converting to set
host_ips_set = set(host_ips)

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    "-o LogLevel=quiet "
    )


def exec_kill():
  cmd = ssh_cmd + ip + " killall -q " + prog_name
  os.system(cmd)
  return

num_cores = multiprocessing.cpu_count()
results = Parallel(n jobs=num_cores)(delayed(exec_kill)(ip) for ip in host_ips)
# for ip in host_ips:
#   cmd = ssh_cmd + ip + " killall -q " + prog_name
#   os.system(cmd)
print "Done killing"
