#!/usr/bin/env python

import os
from os.path import dirname, join
import time

#hostfile_name = "machinefiles/localserver"

#app_dir = dirname(dirname(os.path.realpath(__file__)))
app_dir = os.environ.get('NMF_APP_DIRECTORY', "/media/raajay/ps/bosen/app/NMF")
proj_dir = dirname(dirname(app_dir))

#hostfile = join(proj_dir, hostfile_name)
hostfile = os.environ.get('BOSEN_CONFIG_FILE',
                          join(proj_dir, "machinefiles/localserver"))

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
    )

# Get host IPs
with open(hostfile, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]

for client_id, ip in enumerate(host_ips):
  cmd = ssh_cmd + ip + " "
  cmd += "\'python " + join(app_dir, "script/run_local.py")
  cmd += " %d %s\'" % (client_id, hostfile)
  cmd += " &"
  print cmd
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)
