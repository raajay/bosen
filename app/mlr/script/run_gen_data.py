#!/usr/bin/env python

import os
from os.path import dirname
from os.path import join

app_dir = dirname(dirname(os.path.realpath(__file__)))
proj_dir = dirname(dirname(app_dir))

build_dir = join(proj_dir,"build", "app", "mlr")
prog = join(build_dir, "mlr_gendata_main")

# no trailing /
prefix_path = join(app_dir, "datasets")


#app_dir = dirname(dirname(os.path.realpath(__file__)))
#prog = join(app_dir, "bin", "gen_data_sparse")

# no trailing /
#prefix_path = join(app_dir, "datasets")

params = {
    "num_train": 100
    , "feature_dim": 10
    , "num_partitions": 1
    , "nnz_per_col": 10
    , "one_based": True
    , "beta_sparsity": 0.5
    , "correlation_strength": 0.5
    , "noise_ratio": 0.1
    , "snappy_compressed": "false"
    , "num_labels": 2
    }
params["output_file"] = join(prefix_path, "lr%d_dim%d_s%d_nnz%d") \
    % (params["num_labels"], params["feature_dim"], \
    params["num_train"], params["nnz_per_col"])


env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  )

cmd = env_params + prog
cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])
print cmd
os.system(cmd)
