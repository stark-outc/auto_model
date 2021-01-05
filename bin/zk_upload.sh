#!/usr/bin/env bash
proj_dir=${1}
if [ -z ${1} ];then
    echo 'proj_dir is null'
else
    source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/upload_dir/upload_data.py ${proj_dir}
fi
echo 'success'
