#!/usr/bin/env bash
guest_name=${1}
model_id=${2}
save_name=${3}
if [ -z ${1} ];then
    echo 'please input guest_name'
fi
if [-z ${2} ];then
    echo 'please input model_id'
fi
if [-z ${3} ];then
    echo 'please input save_name'
fi
source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/job_oper/download_predict_data.py ${guest_name} ${model_id} ${save_name}
echo 'download success'

