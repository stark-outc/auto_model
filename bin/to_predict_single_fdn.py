import os
import json
import sys
sys.path.append('/data/projects/fate/python/auto_model')
import time
from utils.mysql_util import MysqlUtil
import re
from utils.log import logger
from utils.config import ZK_DIR,PREDICT_DIR,HOST_TABLE_CONF,DOWNLOAD_DIR,all_table
from utils.file_util import read_data,write_data
import ast
def predict(model_id,guest_name,proj_dir,model_type=None):
    with open(ZK_DIR+all_table, 'rb') as f:
        host_table = json.load(f)
    f.close()
    month_lst = host_table.get('guest_table').get(guest_name).keys()
    jobid_lst = []
    for month_i in month_lst:
        try:
             host_name = host_table.get('host_table').get(proj_dir).get(str(month_i))
        except:
            logger.error(f"{month_i} not in host_table,pleace check the guest_table")
        if model_type:
            with open(PREDICT_DIR+'predict_lr_single_fdn.json', 'rb') as fs:
                predict = json.load(fs)
            fs.close()
        else:
            with open(PREDICT_DIR+'predict_fdn.json', 'rb') as fs:
                predict = json.load(fs)
            fs.close()
        predict['job_parameters']['model_version'] = str(model_id)
        predict['role_parameters']['host']['args']['data']['eval_data'] = [host_name]
        predict['role_parameters']['guest']['args']['data']['eval_data'][0]['name'] = guest_name
        predict['role_parameters']['guest']['args']['data']['eval_data'][0]['namespace'] = 'hzph'
        json_data = json.dumps(predict,indent=4)
        if model_type:
            with open('/data/projects/fate/python/auto_model/predict_model/predict_lr_single_fdn.json', 'w') as fw:
                fw.write(json_data)
            fw.close()
            log_pre = os.popen(
                """source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model/predict_model/ && sh s_predict_lr_single_fdn.sh""")
        else:
            with open('/data/projects/fate/python/auto_model/predict_model/predict.json', 'w') as fw:
                fw.write(json_data)
            fw.close()
            log_pre = os.popen(
                """source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model/predict_model/ && sh s_predict.sh""")

        log =log_pre.readlines()
        job_id_re = re.findall('"jobId": "(.*?)"', str(log))[0]
        job_stat = re.findall('"retmsg": "(.*?)"', str(log))[0]
        if job_stat=='success':
            logger.info(f'{guest_name}_{month_i} predict success and job id is {job_id_re}')
            jobid_lst.append({month_i:job_id_re})
        else:
            logger.error(f'predict_job {month_i} is failed' )
        write_data(PREDICT_DIR+guest_name+'_'+'predict_jobid.txt',str(jobid_lst),method='w')
        time.sleep(80)
    return 1
if __name__ == '__main__':
    model_id = sys.argv[1]
    guest_name = sys.argv[2]
    proj_dir = sys.argv[3]
    model_type = sys.argv[4]
    predict(model_id,guest_name,proj_dir,model_type)

