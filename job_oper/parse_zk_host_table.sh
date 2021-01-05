proj_dir=${1}
if [ -z ${1} ];then
    echo 'proj_dir is null'
else
    source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/predict_model/parse_predict_job.py ${proj_dir}
fi
echo 'success'
