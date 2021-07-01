import os
import json
import sys
sys.path.append('..')
import time
from utils.mysql_util import MysqlUtil
import re
from utils.log import logger
from utils.config import ZK_DIR,PREDICT_DIR,HOST_TABLE_CONF,DOWNLOAD_DIR,ALL_TABLE,PREDICT_SH_LR,PREDICT_JSON_LR,NAMESPACE,PREDICT_JSON_SBT,PREDICT_SH_SBT,VENV,RUNNING_NUM
from utils.file_util import read_data,write_data
from utils.get_model_param import query_status,board_status,get_waiting_job

@board_status
def predict(model_id,guest_name,proj_dir,model_type=None):
    with open(HOST_TABLE_CONF+ALL_TABLE, 'rb') as f:
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
            with open(PREDICT_DIR+PREDICT_JSON_LR, 'rb') as fs:
                predict = json.load(fs)
            fs.close()
        else:
            with open(PREDICT_DIR+PREDICT_JSON_SBT, 'rb') as fs:
                predict = json.load(fs)
            fs.close()
        predict['job_parameters']['model_version'] = str(model_id)
        predict['role_parameters']['host']['args']['data']['train_data_0'] = [host_name]
        predict['role_parameters']['guest']['args']['data']['train_data_0'][0]['name'] = guest_name
        predict['role_parameters']['guest']['args']['data']['train_data_0'][0]['namespace'] = NAMESPACE
        json_data = json.dumps(predict,indent=4)
        if model_type:
            with open(PREDICT_DIR+PREDICT_JSON_LR, 'w') as fw:
                fw.write(json_data)
            fw.close()
            log_pre = os.popen(
                f"""source {VENV} && cd {PREDICT_DIR} && sh {PREDICT_SH_LR}""")
        else:
            with open(PREDICT_DIR+PREDICT_JSON_SBT, 'w') as fw:
                fw.write(json_data)
            fw.close()
            log_pre = os.popen(
                f"""source {VENV} && cd {PREDICT_DIR} && sh {PREDICT_SH_SBT}""")

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
        running_num = query_status()
        waiting_num = len(get_waiting_job())
        to_deal_num = running_num + waiting_num
        wait_circle = 20
        while to_deal_num > RUNNING_NUM:
            if wait_circle > 0 :
                wait_circle -= 1
                time.sleep(30)
                to_deal_num = query_status() + len(get_waiting_job())
                if to_deal_num > RUNNING_NUM:
                    logger.info('board job not finish, run more 30s')
                    continue
            else:
                logger.info('wait more than 10 minutes,please check the board status')
                return
    return 1
if __name__ == '__main__':
    model_id = sys.argv[1]
    guest_name = sys.argv[2]
    proj_dir = sys.argv[3]
    model_type = sys.argv[4]
    # model_id = '202104101554210708684008'
    # guest_name = 'sh_0304'
    # proj_dir = 'sh_0304'
    # model_type = 'lr'
    predict(model_id,guest_name,proj_dir,model_type)
