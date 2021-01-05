# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 17:09:00 2020

@author: 徐钦华
"""
import os
import json
import sys
import time
from utils.mysql_util import MysqlUtil
import re
from utils.log import logger
from utils.config import ZK_DIR,PREDICT_DIR,HOST_TABLE_CONF
from utils.file_util import read_data,write_data
import ast
def predict(model_id,guest_name,proj_dir):
    with open(ZK_DIR+f'{proj_dir}_host_table.json', 'rb') as f:
        host_table = json.load(f)
    f.close()
    month_lst = host_table.get('guest_table').get(guest_name).keys()
    jobid_lst = []
    for month_i in month_lst:
        try:
             host_name = host_table.get('host_table').get(proj_dir).get(str(month_i))
        except:
            logger.error(f"{month_i} not in host_table,pleace check the guest_table ")

        with open(PREDICT_DIR+'predict.json', 'rb') as fs:
            predict = json.load(fs)
        fs.close()
        predict['job_parameters']['model_version'] = str(model_id)
        predict['role_parameters']['host']['args']['data']['data_0'] = [host_name]
        predict['role_parameters']['guest']['args']['data']['data_0'][0]['name'] = guest_name
        json_data = json.dumps(predict)
        with open('/data/projects/fate/python/auto_model/predict_model/predict.json', 'w') as fw:
            fw.write(json_data)
        fw.close()
        log_pre = os.popen(
            """source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model/predict_model/ && sh s_predict.sh""")
        # log_pre = os.popen("""source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model/predict_model/ && python predict.py {} {} {} {}""".format(month,model_id,guest_name,proj_name))
        log =log_pre.readlines()
        job_id_re = re.findall('"jobId": "(.*?)"', str(log))[0]
        job_stat = re.findall('"retmsg": "(.*?)"', str(log))[0]
        if job_stat=='success':
            jobid_lst.append({month_i:job_id_re})
        else:
            logger.error(f'predict_job {month_i} is failed' )
        write_data(PREDICT_DIR+guest_name+'_'+'predict_jobid.txt',str(jobid_lst),method='w')
    return 1


def download_data(guest_name):
    mu = MysqlUtil()
    os.system("""source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/job_oper/get_predict_jobid.py""")
    parse_jobid_sql = """select job_id from predict_jobid"""
    flag, rows, predict_saved_id = mu.select(parse_jobid_sql)
    predict_saved_id_lst = [i[0] for i in predict_saved_id]
    try:
        tmp = read_data(PREDICT_DIR+guest_name+'_'+'predict_jobid.txt')
        lst = ast.literal_eval(tmp)
    except:
        logger.error(f"{guest_name}_predict_jobid.txt is not exists")
    for lst_i in lst:
        print(lst_i)
        download_jobid =list(lst_i.values())[0]
        month = list(lst_i.keys())[0]
        print(download_jobid,month)
        if download_jobid not in predict_saved_id_lst:
            logger.error(f"{download_jobid} not in predict_jobid")
        time.sleep(5)
        try:
            os.system(
                """source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/fate_flow/ && python fate_flow_client.py -f component_output_data -j {} -r guest -p 71000 -cpn secureboost_0 -o /data/projects/fate/python/auto_model/moder_out/{}_model_out/{}_{}""".format(
                    str(download_jobid), guest_name, guest_name, str(month)))
        except:
            logger.error(f"job_id:{lst_i}download failed")
if __name__=='__main__':
    # predict('20201214172759828991360','ms_data','all')
    download_data('ms_data')
    # month = sys.argv[1]
    # guest_name = sys.argv[2]
    # download_data(month,guest_name)
    # '民生'
    # for i in ['201906','201907']:
    #     download_data(i,'20201214180328314862366','zj_dpd','zj')
    # for i in ['201906','201907', '201908','201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004','202005', '202006', '202007', '202008','202009']:
    #     download_data(i,'zj_dpd')
    #
    # for i in ['202001', '202002', '202003', '202004']:
    #     download_data(i,'ms_oot_1')
    #
    # for i in ['201910', '201907', '201909', '201911', '202001', '201912','201908', '201906', '202004', '202006', '202002', '202003','202005', '202007']:
    #     download_data(i,'zbs_data')
    #
    # '众邦'
    # for i in ['201906','201907', '201908','201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004','202005', '202006', '202007', '202008','202009']:
    #     download_data(i,'zj_fpd')
    # for i in ['201906','201907', '201908','201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004','202005', '202006', '202007', '202008','202009']:
    #     download_data(i,'zj_dpd')
    #
    # for i in ['202001', '202002', '202003', '202004', '201912', '201910','201909', '201911', '201908']:
    #     download_data(i,'ms_data')
    #
    # for i in ['202001', '202002', '202003', '202004','202005', '202006', '202007']:
    #     download_data(i,'zbs_oot_1')
    # '借呗'
    # for i in ['202008']:
    #     download_data(i,'20201214180328314862366','oot_j_202008','jb')
    # for i in ['201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004','202005', '202006', '202007', '202008','202009']:
    #     download_data(i,'20201214180328314862366','zj_fpd','zj')
    # for i in ['201906','201907', '201908','201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004','202005', '202006', '202007', '202008','202009']:
    #     download_data(i,'20201214180328314862366','zj_dpd','zj')
    # for i in ['202001', '202002', '202003', '202004', '201912', '201910','201909', '201911', '201908']:
    #     download_data(i,'20201217140001573261748','ms_data','all')
    # for i in ['201910', '201907', '201909', '201911', '202001', '201912','201908', '201906', '202004', '202006', '202002', '202003','202005', '202007']:
    #     download_data(i,'20201217140001573261748','zbs_data','all')
